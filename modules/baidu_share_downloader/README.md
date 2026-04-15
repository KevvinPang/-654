# Baidu Share Downloader

这个模块是已经实测成功的百度网盘分享下载脚本整理版。

特点：

- 复用本机 Edge 登录态
- 支持百度分享链接
- 支持中途断线后续传
- 默认下载到 `E:\服务器自动剪辑\runtime\baidu_downloads`

## 用法

```powershell
python .\baidu_share_downloader.py "https://pan.baidu.com/s/xxxx?pwd=xxxx"
```

指定文件名：

```powershell
python .\baidu_share_downloader.py "https://pan.baidu.com/s/xxxx?pwd=xxxx" --target-filename "7.mp4"
```

只列出 mp4：

```powershell
python .\baidu_share_downloader.py "https://pan.baidu.com/s/xxxx?pwd=xxxx" --list-only
```
