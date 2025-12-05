# 上游仓库为 upstream
# $ git remote add upstream https://github.com/kubesphere/kubesphere.git  
# $ git remote -v
# origin  https://github.com/kit101/kubesphere.git (fetch)
# origin  https://github.com/kit101/kubesphere.git (push)
# upstream        https://github.com/kubesphere/kubesphere (fetch)
# upstream        https://github.com/kubesphere/kubesphere (push)

# 获取最新的 upstream 分支、tag信息
git fetch upstream --prune --tags

# 遍历所有 upstream 分支，创建对应的本地分支
for branch in $(git branch -r | grep 'upstream/' | grep -v 'HEAD'); do
  local_branch="${branch#upstream/}"
  git checkout -b "$local_branch" "$branch"
done

# 推送所有本地分支到 origin
git push origin --all
# 推送所有本地 tag 到 origin
git push origin --tags
