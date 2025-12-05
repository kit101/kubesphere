kubernetes:
  kubeconfig: "./kubeconfig.yaml"
authentication:
  authenticateRateLimiterMaxTries: 10
  authenticateRateLimiterDuration: 10m0s
  loginHistoryRetentionPeriod: 168h
  maximumClockSkew: 10s
  multipleLogin: True
  kubectlImage: kubesphere/kubectl:v1.22.0
  jwtSecret: "youknowiamajwtsecret"
  oauthOptions:
    clients:
    - name: kubesphere
      secret: kubesphere
      redirectURIs:
      - '*'
network:
  ippoolType: none
multicluster:
  clusterRole: none
monitoring:
  endpoint: http://prometheus-operated.kubesphere-monitoring-system.svc:9090
  enableGPUMonitoring: false
gpu:
  kinds:
  - resourceName: nvidia.com/gpu
    resourceType: GPU
    default: True
notification:
  endpoint: http://notification-manager-svc.kubesphere-monitoring-system.svc:19093


terminal:
  image: alpine:3.14
  timeout: 600
gateway:
  watchesPath: ./tmp/var/helm-charts/watches.yaml
  repository: kubesphere/nginx-ingress-controller
  tag: v1.3.1
  namespace: kubesphere-controls-system