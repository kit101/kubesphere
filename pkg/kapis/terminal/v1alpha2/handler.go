/*
Copyright 2020 KubeSphere Authors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package v1alpha2

import (
	"archive/tar"
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	corev1 "k8s.io/api/core/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/remotecommand"
	"mime/multipart"
	"net/http"
	"path/filepath"
	"strconv"
	"strings"

	"kubesphere.io/kubesphere/pkg/api"
	"kubesphere.io/kubesphere/pkg/apiserver/authorization/authorizer"
	requestctx "kubesphere.io/kubesphere/pkg/apiserver/request"

	"github.com/emicklei/go-restful/v3"
	"github.com/gorilla/websocket"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/klog/v2"

	"k8s.io/kubectl/pkg/scheme"
	"kubesphere.io/kubesphere/pkg/models/terminal"
)

var upgrader = websocket.Upgrader{
	ReadBufferSize:  1024,
	WriteBufferSize: 1024,
	// Allow connections from any Origin
	CheckOrigin: func(r *http.Request) bool { return true },
}

type terminalHandler struct {
	client          kubernetes.Interface
	config          *rest.Config
	terminaler      terminal.Interface
	authorizer      authorizer.Authorizer
	uploadFileLimit int64
}

func newTerminalHandler(
	client kubernetes.Interface,
	authorizer authorizer.Authorizer,
	config *rest.Config,
	options *terminal.Options) *terminalHandler {

	var uploadFileLimit int64 = 100 << 20 // 100 MB
	q, err := resource.ParseQuantity(options.UploadFileLimit)
	if err != nil {
		klog.Warningf("parse UploadFileLimit failed: %s, using default value 100Mi", err.Error())
	} else {
		uploadFileLimit = q.Value()
	}

	return &terminalHandler{
		client:          client,
		config:          config,
		authorizer:      authorizer,
		terminaler:      terminal.NewTerminaler(client, config, options),
		uploadFileLimit: uploadFileLimit,
	}
}

func (t *terminalHandler) handleTerminalSession(request *restful.Request, response *restful.Response) {
	namespace := request.PathParameter("namespace")
	podName := request.PathParameter("pod")
	containerName := request.QueryParameter("container")
	shell := request.QueryParameter("shell")

	user, _ := requestctx.UserFrom(request.Request.Context())

	createPodsExec := authorizer.AttributesRecord{
		User:            user,
		Verb:            "create",
		Resource:        "pods",
		Subresource:     "exec",
		Namespace:       namespace,
		ResourceRequest: true,
		ResourceScope:   requestctx.NamespaceScope,
	}

	decision, reason, err := t.authorizer.Authorize(createPodsExec)
	if err != nil {
		api.HandleInternalError(response, request, err)
		return
	}

	if decision != authorizer.DecisionAllow {
		api.HandleForbidden(response, request, fmt.Errorf(reason))
		return
	}

	conn, err := upgrader.Upgrade(response.ResponseWriter, request.Request, nil)
	if err != nil {
		klog.Warning(err)
		return
	}

	t.terminaler.HandleSession(shell, namespace, podName, containerName, conn)
}

func (t *terminalHandler) handleShellAccessToNode(request *restful.Request, response *restful.Response) {
	nodename := request.PathParameter("nodename")

	user, _ := requestctx.UserFrom(request.Request.Context())

	createNodesExec := authorizer.AttributesRecord{
		User:            user,
		Verb:            "create",
		Resource:        "nodes",
		Subresource:     "exec",
		ResourceRequest: true,
		ResourceScope:   requestctx.ClusterScope,
	}

	decision, reason, err := t.authorizer.Authorize(createNodesExec)
	if err != nil {
		api.HandleInternalError(response, request, err)
		return
	}

	if decision != authorizer.DecisionAllow {
		api.HandleForbidden(response, request, fmt.Errorf(reason))
		return
	}

	conn, err := upgrader.Upgrade(response.ResponseWriter, request.Request, nil)
	if err != nil {
		klog.Warning(err)
		return
	}

	t.terminaler.HandleShellAccessToNode(nodename, conn)
}

// FileItem 表示一个文件/目录的结构化信息
type FileItem struct {
	Name    string `json:"name"`
	AbsPath string `json:"absPath"`          // 绝对路径
	Type    string `json:"type"`             // "file", "dir", "link"
	Mode    string `json:"mode"`             // 权限字符串，如 "drwxr-xr-x"
	Size    string `json:"size"`             // 人类可读大小，如 "1.2K"
	LinkTo  string `json:"linkTo,omitempty"` // 如果是软链接，指向的目标
	ModTime string `json:"modTime"`          // 修改时间（保持 ls 原始格式）
}

// parseLsLine 解析 `ls -alh` 的单行输出
// 示例: drwxr-xr-x 2 root root 4.0K Dec  9 10:30 console
func parseLsLine(line string) (FileItem, error) {
	fields := strings.Fields(line)
	if len(fields) < 9 {
		return FileItem{}, fmt.Errorf("invalid ls line format: %s", line)
	}

	mode := fields[0]
	nameStart := 8 // 前8个字段是：mode, links, user, group, size, month, day, time/year

	// 处理软链接：name -> target
	name := fields[nameStart]
	var linkTo string
	if strings.HasSuffix(mode, "@") && len(fields) > nameStart+1 && fields[nameStart+1] == "->" {
		linkTo = strings.Join(fields[nameStart+2:], " ")
		mode = strings.TrimSuffix(mode, "@")
	} else if strings.Contains(name, "->") {
		parts := strings.SplitN(name, "->", 2)
		name = strings.TrimSpace(parts[0])
		linkTo = strings.TrimSpace(parts[1])
	}

	// 判断类型
	var ftype string
	switch mode[0] {
	case 'd':
		ftype = "dir"
	case 'l':
		ftype = "link"
	default:
		ftype = "file"
	}

	modTime := strings.Join(fields[5:8], " ")

	return FileItem{
		Name:    name,
		Type:    ftype,
		Mode:    mode,
		Size:    fields[4],
		LinkTo:  linkTo,
		ModTime: modTime,
	}, nil
}

func listFilesWithLs(
	request *restful.Request,
	h *terminalHandler,
	podName, namespace, containerName, targetDir string,
) ([]FileItem, error) {
	req := h.client.CoreV1().RESTClient().Post().
		Resource("pods").
		Name(podName).
		Namespace(namespace).
		SubResource("exec").
		VersionedParams(&corev1.PodExecOptions{
			Container: containerName,
			Command:   []string{"ls", "-alh", targetDir},
			Stdin:     false,
			Stdout:    true,
			Stderr:    true,
			TTY:       false,
		}, scheme.ParameterCodec)

	exec, err := remotecommand.NewSPDYExecutor(h.config, "POST", req.URL())
	if err != nil {
		return nil, fmt.Errorf("failed to create executor: %v", err)
	}

	var stdout, stderr bytes.Buffer
	err = exec.StreamWithContext(request.Request.Context(), remotecommand.StreamOptions{
		Stdin:  nil,
		Stdout: &stdout,
		Stderr: &stderr,
	})
	if err != nil {
		errMsg := stderr.String()
		if errMsg == "" {
			errMsg = err.Error()
		}
		return nil, fmt.Errorf("failed to list files in %s/%s (%s): %s", namespace, podName, targetDir, errMsg)
	}

	lines := strings.Split(strings.TrimSpace(stdout.String()), "\n")
	if len(lines) == 0 {
		return nil, fmt.Errorf("empty output from ls command")
	}

	start := 0
	if strings.HasPrefix(lines[0], "total") {
		start = 1
	}

	var files []FileItem
	for _, line := range lines[start:] {
		if line == "" {
			continue
		}
		item, err := parseLsLine(line)
		if err != nil {
			klog.Warningf("Failed to parse ls line: %s, error: %v", line, err)
			continue
		}
		files = append(files, item)
	}
	return files, nil
}

func listFilesWithStat(
	request *restful.Request,
	h *terminalHandler,
	podName, namespace, containerName, targetDir string,
) ([]FileItem, error) {
	cmd := []string{
		"sh", "-c",
		fmt.Sprintf(
			`find %q -maxdepth 1 -exec stat -c '{"name":"%%n", "size":%%s, "mode":"%%A", "type":"%%F", "mtime":"%%Y", "linkTo": "%%N"}' {} \; 2>/dev/null`,
			targetDir,
		),
	}

	req := h.client.CoreV1().RESTClient().Post().
		Resource("pods").
		Name(podName).
		Namespace(namespace).
		SubResource("exec").
		VersionedParams(&corev1.PodExecOptions{
			Container: containerName,
			Command:   cmd,
			Stdin:     false,
			Stdout:    true,
			Stderr:    true,
			TTY:       false,
		}, scheme.ParameterCodec)

	exec, err := remotecommand.NewSPDYExecutor(h.config, "POST", req.URL())
	if err != nil {
		return nil, err
	}

	var stdout, stderr bytes.Buffer
	err = exec.StreamWithContext(request.Request.Context(), remotecommand.StreamOptions{
		Stdout: &stdout,
		Stderr: &stderr,
	})
	if err != nil {
		return nil, fmt.Errorf("exec failed: %w, stderr: %s", err, stderr.String())
	}

	var files []FileItem
	lines := strings.Split(strings.TrimSpace(stdout.String()), "\n")
	for _, line := range lines {
		if line == "" {
			continue
		}
		var raw struct {
			Name   string `json:"name"`
			Size   int64  `json:"size"`
			Mode   string `json:"mode"`
			Type   string `json:"type"`
			Mtime  string `json:"mtime"`
			LinkTo string `json:"linkTo"`
		}
		if err := json.Unmarshal([]byte(line), &raw); err != nil {
			continue
		}

		// 转换 type
		var ftype string
		switch raw.Type {
		case "directory":
			ftype = "dir"
		case "symbolic link":
			ftype = "link"
			// 获取链接目标（需额外命令，此处暂留空；实际可 fallback 到 ls）
		case "regular file", "regular empty file":
			ftype = "file"
		default:
			ftype = "file"
		}

		// 转换 size 为人类可读（简化版，也可用 humanize 包）
		sizeStr := humanizeFileSize(raw.Size)

		// 提取 basename（因为 find 返回绝对路径）
		name := filepath.Base(raw.Name)

		files = append(files, FileItem{
			Name:    name,
			AbsPath: raw.Name,
			Type:    ftype,
			Mode:    raw.Mode,
			Size:    sizeStr,
			LinkTo:  raw.LinkTo,
			ModTime: raw.Mtime,
		})
	}
	return files, nil
}

func humanizeFileSize(
	s int64,
) string {
	const (
		_  = iota
		KB = 1 << (10 * iota)
		MB
		GB
		TB
	)
	if s < KB {
		return strconv.FormatInt(s, 10)
	} else if s < MB {
		return fmt.Sprintf("%.1fK", float64(s)/KB)
	} else if s < GB {
		return fmt.Sprintf("%.1fM", float64(s)/MB)
	} else if s < TB {
		return fmt.Sprintf("%.1fG", float64(s)/GB)
	}
	return fmt.Sprintf("%.1fT", float64(s)/TB)
}

func listFilesWithFindLs(
	request *restful.Request,
	h *terminalHandler,
	podName, namespace, containerName, targetDir string,
) ([]FileItem, error) {
	cmd := []string{
		"sh", "-c",
		fmt.Sprintf(
			`find %q -maxdepth 1 -print0 | xargs -0 -I {} ls -ld {}`,
			targetDir,
		),
	}

	req := h.client.CoreV1().RESTClient().Post().
		Resource("pods").
		Name(podName).
		Namespace(namespace).
		SubResource("exec").
		VersionedParams(&corev1.PodExecOptions{
			Container: containerName,
			Command:   cmd,
			Stdin:     false,
			Stdout:    true,
			Stderr:    true,
			TTY:       false,
		}, scheme.ParameterCodec)

	exec, err := remotecommand.NewSPDYExecutor(h.config, "POST", req.URL())
	if err != nil {
		return nil, err
	}

	var stdout, stderr bytes.Buffer
	err = exec.StreamWithContext(request.Request.Context(), remotecommand.StreamOptions{
		Stdout: &stdout,
		Stderr: &stderr,
	})
	if err != nil {
		return nil, fmt.Errorf("exec failed: %w, stderr: %s", err, stderr.String())
	}

	var files []FileItem
	lines := strings.Split(strings.TrimSpace(stdout.String()), "\n")
	for _, line := range lines {
		if line == "" {
			continue
		}
		f, err := parseFindLsLine(line)
		if err != nil {
			// 跳过无效行（如权限错误）
			continue
		}
		// 过滤自身（targetPath 自己）
		if f.Name == targetDir {
			continue
		}
		files = append(files, f)
	}
	return files, nil
}

// parseLsLine 解析 `ls -ld` 的标准输出（POSIX 兼容）
func parseFindLsLine(line string) (FileItem, error) {
	fields := strings.Fields(line)
	if len(fields) < 8 {
		return FileItem{}, fmt.Errorf("invalid ls line: %s", line)
	}

	mode := fields[0]
	size := fields[4]
	// 日期字段：可能是 "Dec 10 10:00" 或 "Dec 10  2024"
	var nameStart int
	if strings.Contains(fields[7], ":") || len(fields) > 8 {
		nameStart = 7
	} else {
		nameStart = 6
	}
	name := strings.Join(fields[nameStart:], " ")

	// 类型
	var ftype string
	switch mode[0] {
	case 'd':
		ftype = "dir"
	case 'l':
		ftype = "symlink"
	default:
		ftype = "file"
	}

	// 处理软链接目标
	var linkTo string
	if ftype == "symlink" {
		if parts := strings.SplitN(name, " -> ", 2); len(parts) == 2 {
			name = parts[0]
			linkTo = parts[1]
		}
	}

	// 解析大小
	sizeInt, _ := strconv.ParseInt(size, 10, 64)

	return FileItem{
		Name:    name,
		Type:    ftype,
		Mode:    mode,
		Size:    humanizeFileSize(sizeInt),
		LinkTo:  linkTo,
		ModTime: strings.Join(fields[5:nameStart], " "),
	}, nil
}

func (h *terminalHandler) List(request *restful.Request, response *restful.Response) {
	targetDir := request.QueryParameter("path")
	if targetDir == "" {
		targetDir = "/"
	}

	namespace := request.PathParameter("namespace")
	podName := request.PathParameter("pod")
	containerName := request.QueryParameter("container")
	bin := request.QueryParameter("bin")

	if bin == "" {
		bin = "ls" // 默认使用 ls，兼容性最好
	}

	var files []FileItem
	var err error

	switch bin {
	case "ls":
		files, err = listFilesWithLs(request, h, podName, namespace, containerName, targetDir)
	case "findLs":
		files, err = listFilesWithFindLs(request, h, podName, namespace, containerName, targetDir)
	case "stat":
		files, err = listFilesWithStat(request, h, podName, namespace, containerName, targetDir)
	default:
		err = fmt.Errorf("unsupported bin: %s, use 'ls' or 'find'", bin)
		return
	}

	if err != nil {
		api.HandleInternalError(response, request, err)
		return
	}

	result := map[string]interface{}{
		"items": files,
	}
	response.WriteEntity(result)
}

type fileWithHeader struct {
	file   multipart.File
	header *multipart.FileHeader
}

func (h *terminalHandler) UploadFile(request *restful.Request, response *restful.Response) {
	if err := request.Request.ParseMultipartForm(h.uploadFileLimit); err != nil {
		api.HandleInternalError(response, nil, err)
		return
	}

	files := make([]fileWithHeader, 0)
	for name := range request.Request.MultipartForm.File {
		file, header, err := request.Request.FormFile(name)
		if err != nil {
			api.HandleBadRequest(response, nil, err)
			return
		}
		files = append(files, fileWithHeader{
			file:   file,
			header: header,
		})
	}

	reader, writer := io.Pipe()
	go func() {
		defer writer.Close()

		tarWriter := tar.NewWriter(writer)
		defer tarWriter.Close()

		for _, f := range files {
			func(f fileWithHeader) {
				defer f.file.Close()

				// Write the tar header to the tar file
				if err := tarWriter.WriteHeader(&tar.Header{
					Name: f.header.Filename,
					Mode: 0600,
					Size: f.header.Size,
				}); err != nil {
					api.HandleInternalError(response, nil, err)
					return
				}
				// Copy the file content to the tar file
				if _, err := io.Copy(tarWriter, f.file); err != nil {
					api.HandleInternalError(response, nil, err)
					return
				}
			}(f)
		}
	}()

	targetDir := request.QueryParameter("path")
	if targetDir == "" {
		targetDir = "/"
	}

	namespace := request.PathParameter("namespace")
	podName := request.PathParameter("pod")
	containerName := request.QueryParameter("container")

	req := h.client.CoreV1().RESTClient().Post().
		Resource("pods").
		Name(podName).
		Namespace(namespace).
		SubResource("exec").
		VersionedParams(&corev1.PodExecOptions{
			Container: containerName,
			Command:   []string{"tar", "-xmf", "-", "-C", targetDir},
			Stdin:     true,
			Stdout:    false,
			Stderr:    true,
			TTY:       false,
		}, scheme.ParameterCodec)

	exec, err := remotecommand.NewSPDYExecutor(h.config, "POST", req.URL())
	if err != nil {
		api.HandleInternalError(response, nil, err)
		return
	}

	// 1. 创建缓冲区捕获 stderr 而非直接写入 response
	var stderrBuf bytes.Buffer
	stderrWriter := bufio.NewWriter(&stderrBuf)

	// 执行命令
	streamErr := exec.StreamWithContext(request.Request.Context(), remotecommand.StreamOptions{
		Stdin:  reader,
		Stdout: nil,
		Stderr: stderrWriter,
	})

	// 确保缓冲区内容被写入
	stderrWriter.Flush()
	stderrOutput := strings.TrimSpace(stderrBuf.String())

	// 1. 正确获取退出码 - 使用 Kubernetes 标准方式
	exitCode := getExitCode(streamErr)

	// 2. 检查 stderr 是否包含错误信息（即使退出码为0）
	if stderrOutput != "" && exitCode == 0 {
		// 检查是否包含常见错误关键词
		if strings.Contains(stderrOutput, "Permission denied") ||
			strings.Contains(stderrOutput, "No such file") ||
			strings.Contains(stderrOutput, "cannot open") {
			exitCode = 1
		}
	}

	// 3. 处理失败情况
	if exitCode != 0 || streamErr != nil {
		errorMsg := buildErrorMessage(streamErr, stderrOutput, exitCode)
		klog.Warningf("Upload failed: %s", errorMsg)

		// 返回4xx状态码
		api.HandleBadRequest(response, nil, fmt.Errorf("upload failed: %s", errorMsg))
		return
	}

}

// getExitCode 从Kubernetes错误中提取退出码
func getExitCode(err error) int {
	if err == nil {
		return 0
	}

	// 1. 检查是否是API服务器返回的StatusError
	if statusErr, ok := err.(*k8serrors.StatusError); ok {
		status := statusErr.ErrStatus
		if status.Code != 0 {
			return int(status.Code)
		}
		// 检查详细原因
		if status.Reason == metav1.StatusReasonUnauthorized {
			return http.StatusUnauthorized
		}
		if status.Reason == metav1.StatusReasonForbidden {
			return http.StatusForbidden
		}
	}

	// 2. 检查是否是SPDY执行错误
	if spdyErr, ok := err.(interface{ ExitStatus() int }); ok {
		return spdyErr.ExitStatus()
	}

	// 3. 无法识别的错误
	klog.Warningf("Unknown error type: %T", err)
	return 1
}

// buildErrorMessage 构建用户友好的错误消息
func buildErrorMessage(err error, stderr string, exitCode int) string {
	messages := []string{}

	// 添加stderr内容（如果存在），但过滤掉敏感信息
	if stderr != "" {
		// 提取关键错误行
		lines := strings.Split(stderr, "\n")
		for _, line := range lines {
			// 过滤包含敏感信息的行
			if strings.Contains(line, "https://") || strings.Contains(line, "Post \"") || strings.Contains(line, "URL=") {
				// 替换为通用错误信息
				messages = append(messages, "execution failed: error sending request to cluster")
				break
			}
			
			if strings.Contains(line, "error") ||
				strings.Contains(line, "fail") ||
				strings.Contains(line, "denied") ||
				strings.Contains(line, "no such") {
				messages = append(messages, line)
				break // 只取第一个关键错误
			}
		}
	}

	// 如果没有从stderr中提取到信息，则添加通用错误信息
	if len(messages) == 0 && err != nil {
		// 检查是否是网络连接错误
		if strings.Contains(err.Error(), "EOF") || strings.Contains(err.Error(), "connection") {
			messages = append(messages, "execution failed: error sending request to cluster")
		} else {
			// 对于其他错误，只显示简单的描述
			messages = append(messages, "execution failed")
		}
	}

	// 合并消息
	if len(messages) == 0 {
		return "unknown upload failure"
	}
	
	// 最多只返回前两条错误信息，避免信息过多
	if len(messages) > 2 {
		messages = messages[:2]
	}
	
	return strings.Join(messages, "\n")
}

func (h *terminalHandler) DownloadFile(request *restful.Request, response *restful.Response) {
	filePath := request.QueryParameter("path")
	fileName := filepath.Base(filePath)

	response.AddHeader("Content-Disposition", fmt.Sprintf("attachment; filename=%s.tar", fileName))

	namespace := request.PathParameter("namespace")
	podName := request.PathParameter("pod")
	containerName := request.QueryParameter("container")

	reader, err := newTarPipe(request.Request.Context(), h.config, h.client.CoreV1().RESTClient(), namespace, podName, containerName, filePath)
	if err != nil {
		api.HandleInternalError(response, nil, err)
		return
	}

	if _, err = io.Copy(response.ResponseWriter, reader); err != nil {
		api.HandleInternalError(response, nil, err)
		return
	}
}

type tarPipe struct {
	config *rest.Config
	client rest.Interface

	reader    *io.PipeReader
	outStream *io.PipeWriter
	bytesRead uint64
	size      uint64
	ctx       context.Context

	namespace, name, container, filePath string
}

func newTarPipe(ctx context.Context, config *rest.Config, client rest.Interface, namespace, name, container, filePath string) (*tarPipe, error) {
	t := &tarPipe{
		config:    config,
		client:    client,
		namespace: namespace,
		name:      name,
		container: container,
		filePath:  filePath,
		ctx:       ctx,
	}

	if err := t.getFileSize(); err != nil {
		return nil, err
	}
	if err := t.initReadFrom(0); err != nil {
		return nil, err
	}
	return t, nil
}

func (t *tarPipe) getFileSize() error {
	req := t.client.Post().
		Resource("pods").
		Name(t.name).
		Namespace(t.namespace).
		SubResource("exec").
		VersionedParams(&corev1.PodExecOptions{
			Container: t.container,
			Command:   []string{"sh", "-c", fmt.Sprintf("tar cf - %s | wc -c", t.filePath)},
			Stdin:     false,
			Stdout:    true,
			Stderr:    false,
			TTY:       false,
		}, scheme.ParameterCodec)

	exec, err := remotecommand.NewSPDYExecutor(t.config, "POST", req.URL())
	if err != nil {
		return err
	}

	reader, writer := io.Pipe()
	go func() {
		defer writer.Close()

		if err = exec.StreamWithContext(t.ctx, remotecommand.StreamOptions{
			Stdin:             nil,
			Stdout:            writer,
			Stderr:            nil,
			TerminalSizeQueue: nil,
		}); err != nil {
			klog.Error(err)
		}
	}()

	result, err := io.ReadAll(reader)
	if err != nil {
		return err
	}
	num, err := strconv.ParseUint(strings.TrimSpace(string(result)), 10, 64)
	if err != nil {
		return err
	}
	t.size = num
	return nil
}

func (t *tarPipe) initReadFrom(n uint64) error {
	t.reader, t.outStream = io.Pipe()

	req := t.client.Post().
		Resource("pods").
		Name(t.name).
		Namespace(t.namespace).
		SubResource("exec").
		VersionedParams(&corev1.PodExecOptions{
			Container: t.container,
			Command:   []string{"sh", "-c", fmt.Sprintf("tar cf - %s | tail -c+%d", t.filePath, n)},
			Stdin:     false,
			Stdout:    true,
			Stderr:    false,
			TTY:       false,
		}, scheme.ParameterCodec)

	exec, err := remotecommand.NewSPDYExecutor(t.config, "POST", req.URL())
	if err != nil {
		return err
	}

	go func() {
		defer t.outStream.Close()

		if err = exec.StreamWithContext(t.ctx, remotecommand.StreamOptions{
			Stdin:             nil,
			Stdout:            t.outStream,
			Stderr:            nil,
			TerminalSizeQueue: nil,
		}); err != nil {
			klog.Error(err)
		}
	}()
	return nil
}

func (t *tarPipe) Read(p []byte) (int, error) {
	n, err := t.reader.Read(p)
	if err != nil {
		if t.bytesRead == t.size {
			return n, io.EOF
		}
		return n, t.initReadFrom(t.bytesRead + 1)
	}
	t.bytesRead += uint64(n)
	return n, nil
}
