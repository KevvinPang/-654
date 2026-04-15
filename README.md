# 服务器自动剪辑

这个目录现在已经整理成统一工程，当前已整合的可用模块有：

- 百度网盘分享链接下载
- 抖音分享链接下载
- 字幕提取
- 自动剪辑成片

现在这套结构已经不是单独的脚本集合，而是可以把本地电脑长期挂着当“本地服务器”的工作底座。后面如果你继续补发包、发布、审核之类的新模块，可以继续往这里接。

## 当前已经就绪的能力

- 支持按工作间保存任务配置
- 支持多个工作间并发运行
- 支持一个工作间里同时丢多个下载、字幕、剪辑任务
- 提供本地浏览器控制台，统一查看模块、编辑任务、启动任务、看日志
- 字幕提取默认使用 `accurate`
- 字幕提取默认抽帧频率为 `5`
- 自动字幕区检测优先走共享图像检测，OCR 作为兜底
- 自动剪辑模块已经接入统一批处理，可以直接从工作间任务里调用

## 目录结构

```text
E:\服务器自动剪辑
├─ batch_runner.py
├─ control_center.py
├─ start_control_center.bat
├─ start_control_center.ps1
├─ prepare_server_env.bat
├─ prepare_server_env.ps1
├─ init_douyin_env.ps1
├─ init_subtitle_env.ps1
├─ init_auto_clip_env.ps1
├─ module_manifest.json
├─ control_center_manifest.json
├─ modules
│  ├─ auto_clip_engine
│  │  ├─ drama_clone_cli.py
│  │  ├─ drama_clone_core.py
│  │  └─ requirements.txt
│  ├─ baidu_share_downloader
│  ├─ douyin_api
│  ├─ douyin_batch_downloader.py
│  ├─ subtitle_extractor
│  ├─ subtitle_extractor_source
│  ├─ subtitle_region_detector.py
│  └─ subtitle_batch_runner.py
├─ runtime
│  └─ workspaces
│     └─ demo_workspace
│        └─ task.json
└─ docs
   └─ workspace-task.example.json
```

## 第一次准备环境

第一次使用建议先跑：

```powershell
.\prepare_server_env.ps1
```

如果你更习惯双击：

```text
prepare_server_env.bat
```

这个准备脚本会依次安装：

- 抖音下载环境
- 字幕提取环境
- 自动剪辑环境

目前会优先选用本机的 Python `3.11`，其次 `3.12`。

## 启动控制台

直接双击：

```text
start_control_center.bat
```

或者命令行运行：

```powershell
.\start_control_center.ps1
```

启动后会自动打开默认浏览器，默认地址：

```text
http://127.0.0.1:19081
```

## 工作间机制

每个工作间都是一个独立任务目录，建议放在：

```text
E:\服务器自动剪辑\runtime\workspaces\<工作间名>\
```

批处理运行时会自动准备这些目录：

- `downloads\baidu`
- `downloads\douyin`
- `subtitles`
- `clips`
- `logs`
- `temp`

这样下载结果、字幕、成片和日志都不会混在一起。

## 任务配置结构

参考：

- [workspace-task.example.json](/E:/服务器自动剪辑/docs/workspace-task.example.json)
- [task.json](/E:/服务器自动剪辑/runtime/workspaces/demo_workspace/task.json)

现在支持 4 个任务段：

- `baidu_share`
- `douyin_download`
- `subtitle_extract`
- `auto_clip`

并发配置示例：

```json
"concurrency": {
  "baidu_share": 1,
  "douyin_download": 3,
  "subtitle_extract": 1,
  "auto_clip": 1
}
```

如果你不想在每个 `auto_clip` 任务里重复写 AI 设置，可以把它们放在顶层 `settings` 里。

## 字幕提取说明

字幕提取推荐直接开自动检测：

```json
{
  "auto_detect_subtitle_area": true,
  "language": "ch",
  "mode": "accurate",
  "extract_frequency": 5,
  "probe_extract_frequency": 5
}
```

自动检测流程是：

1. 先用 [subtitle_region_detector.py](/E:/服务器自动剪辑/modules/subtitle_region_detector.py) 做图像级字幕区检测
2. 如果图像检测拿不到稳定区域，再回退到 OCR probe
3. 最终输出 `.srt`，同时生成检测预览图和检测报告

## 自动剪辑说明

自动剪辑模块调用的是已经整合进工程的 CLI：

- [drama_clone_cli.py](/E:/服务器自动剪辑/modules/auto_clip_engine/drama_clone_cli.py)
- [drama_clone_core.py](/E:/服务器自动剪辑/modules/auto_clip_engine/drama_clone_core.py)

`auto_clip` 任务至少需要这些输入：

- `reference_video` 或 `reference_video_glob`
- `reference_subtitle` 或 `reference_subtitle_glob`
- `source_dir`

常见写法：

```json
{
  "reference_video_glob": "downloads/douyin/*.mp4",
  "reference_subtitle_glob": "subtitles/*.srt",
  "source_dir": "downloads/baidu",
  "output_subdir": "clips",
  "temp_subdir": "temp/auto_clip",
  "title": "demo_workspace_final",
  "skip_existing": true
}
```

这个阶段已经包含：

- 参考字幕解析
- AI 改写
- TTS 配音
- 参考帧匹配
- 画面重建
- 最终成片合成

## 批量运行

跑所有工作间：

```powershell
.\run_batch_runner.ps1
```

只跑一个工作间：

```powershell
.\run_batch_runner.ps1 -Workspace demo_workspace
```

手动指定任务文件：

```powershell
.\run_batch_runner.ps1 -Config E:\服务器自动剪辑\runtime\workspaces\demo_workspace\task.json
```

## 当前默认并发

- 百度网盘：`1`
- 抖音下载：`3`
- 字幕提取：`1`
- 自动剪辑：`1`
- 工作间并发：`2`

如果后面换更强的机器，可以继续往上调，例如：

```powershell
.\run_batch_runner.ps1 -AllWorkspaces -GlobalDouyinDownload 5 -GlobalSubtitleExtract 2 -GlobalAutoClip 2
```

## 后续接模块建议

- 新模块代码放进 `E:\服务器自动剪辑\modules`
- 新模块输出尽量仍然写进各自工作间
- 控制台展示信息写进 [control_center_manifest.json](/E:/服务器自动剪辑/control_center_manifest.json)
- 统一调度逻辑继续扩展 [batch_runner.py](/E:/服务器自动剪辑/batch_runner.py)

这样以后继续加模块时，不需要再重搭外壳。
