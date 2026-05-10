# 短剧自动剪辑控制台

这是一个面向短剧二创/复刻剪辑流程的本地自动化工作台。软件以“参考视频”为目标模板，自动下载或导入原素材与参考视频，提取参考字幕和参考音频节奏，重建画面片段，生成 TTS 解说，保留人物对白，并混入匹配的本地 BGM，最终输出可直接检查的成片和 SRT。

项目当前已经不是单个脚本集合，而是一个可长期运行的本地控制台：

- 用工作间隔离不同短剧项目。
- 用控制台统一配置下载、字幕、BGM、AI、TTS 和成片任务。
- 用批处理调度器串联百度网盘、抖音参考视频、音频字幕、视觉字幕、自动剪辑等阶段。
- 用回归测试保护字幕融合、TTS 停顿、对白帧匹配、输出规格等关键逻辑。

> 说明：仓库只保存软件代码与配置逻辑，不包含大模型权重、素材、成片、缓存、日志和本地 BGM 库。这些大文件默认由 `.gitignore` 排除。

## 当前能力

- 本地 Web 控制台：双击启动后在浏览器里管理工作间、任务配置、运行日志、素材统计和 AI 设置。
- 多工作间调度：支持多个工作间并发，每个工作间拥有独立的下载、字幕、临时文件、成片和日志目录。
- 百度网盘原素材导入：默认交给官方百度网盘客户端处理下载，并监控/搬运指定文件，减少手动选择与重复任务。
- 抖音参考视频下载：支持按分享链接批量下载参考视频到工作间目录。
- 音频字幕提取：默认以音频识别结果作为正文与停顿主依据，优先使用 Qwen3-ASR + ForcedAligner；不可用时回退 SenseVoice/FunASR 链路。
- 视觉字幕提取：用于参考视频硬字幕 OCR，主要辅助断句、分组、错字纠正和少量漏字补齐。
- 双字幕融合：音频字幕负责内容和停顿主线，视觉字幕只做辅助修复，避免整句插入、整句删除、跨停顿边界合并等问题。
- 音色/类型识别：基于参考音频区分解说、对白、原字幕、水印等类型，尽量让 TTS 与原素材对白各归其位。
- TTS 解说生成：使用 Edge TTS，支持备用 Azure TTS；生成后按参考停顿规划时间线，尽量避免忽快忽慢、吞尾音和与对白重叠。
- 对白区域帧匹配：视觉匹配为主，在对白窗口使用音频内容辅助校验和重排，处理画面相似但台词不同的片段。
- 输出规格继承原素材：成片分辨率和帧率优先来自匹配到的原素材，而不是盲目跟随参考视频规格。
- 字幕遮挡与烧录：自动检测原视频字幕区，遮挡旧字幕并烧录新字幕，同时导出最终 SRT。
- 本地 BGM 自动匹配：联网搜歌流程已移除，改为扫描本地 BGM 库；先做 Demucs 参考 BGM 分离，再用 Chromaprint 找同曲/简单变速版本，最后用本地音频特征与 AudioMuse 风格向量找相似音乐。
- BGM 音量对齐：根据参考视频与候选 BGM 的响度特征估算默认音量，并在 TTS/对白区做 ducking，避免 BGM 抢人声。
- AI 轻量纠错/改写：全工作间共享 AI 设置，默认模型为 `doubao-seed-2-0-lite-260215`；AI 主要用于错别字和轻微顺口修正，尽量不破坏参考停顿和原意。

## 核心流程

```text
创建/选择工作间
  -> 导入或下载原素材
  -> 下载参考视频
  -> 提取音频字幕（Qwen3-ASR 优先）
  -> 提取视觉字幕（硬字幕 OCR）
  -> 融合字幕与类型识别
  -> 按参考视频做画面匹配
  -> 对白窗口音频辅助校验
  -> 生成/对齐 TTS
  -> 自动匹配本地 BGM
  -> 遮挡旧字幕并烧录新字幕
  -> 输出 MP4 + SRT + 日志
```

当前默认策略是“音频主导、视觉辅助”：

- 音频字幕优先决定正文、停顿、TTS 时间线和参考音频节奏。
- 视觉字幕优先用于显示分组、少量漏字补齐、OCR/ASR 错字互相纠正。
- 当视觉字幕缺整句时，音频字幕仍保留该句。
- 当视觉字幕与音频字幕冲突较大时，软件会避免让视觉字幕整句覆盖音频主线。
- AI 修正发生在字幕融合之后，只允许高置信度、低风险的轻改动。

## 目录结构

```text
E:\服务器自动剪辑
├─ batch_runner.py
├─ control_center.py
├─ control_center_ui.html
├─ start_control_center.bat
├─ start_control_center.ps1
├─ prepare_server_env.bat
├─ prepare_server_env.ps1
├─ run_batch_runner.ps1
├─ modules
│  ├─ auto_clip_engine
│  │  ├─ drama_clone_cli.py
│  │  ├─ drama_clone_core.py
│  │  ├─ funasr_subtitle_cli.py
│  │  ├─ funasr_transcribe_helper.py
│  │  ├─ qwen_asr_transcribe_helper.py
│  │  └─ requirements.txt
│  ├─ baidu_share_downloader
│  ├─ baidu_official_client_handoff.py
│  ├─ douyin_api
│  ├─ douyin_batch_downloader.py
│  ├─ subtitle_batch_runner.py
│  └─ subtitle_region_detector.py
├─ runtime
│  └─ workspaces
│     └─ <工作间名>
├─ tests
└─ docs
   └─ workspace-task.example.json
```

每个工作间运行时会自动维护这些目录：

```text
runtime\workspaces\<工作间名>\
├─ downloads\baidu      原素材
├─ downloads\douyin     参考视频
├─ subtitles\audio      音频识别 SRT
├─ subtitles\visual     视觉 OCR SRT
├─ bgm                  工作间私有 BGM
├─ clips                成片输出
├─ temp                 临时文件、job、阶段日志
└─ logs                 工作间日志
```

## 快速开始

首次准备环境：

```powershell
.\prepare_server_env.ps1
```

或双击：

```text
prepare_server_env.bat
```

启动控制台：

```powershell
.\start_control_center.ps1
```

或双击：

```text
start_control_center.bat
```

默认访问地址：

```text
http://127.0.0.1:19081
```

批量运行工作间：

```powershell
.\run_batch_runner.ps1
```

只运行指定工作间：

```powershell
.\run_batch_runner.ps1 -Workspace 工作间名
```

## 工作间配置

工作间配置保存在：

```text
runtime\workspaces\<工作间名>\task.json
```

主要任务段：

- `baidu_share`：百度网盘原素材下载/交接。
- `douyin_download`：抖音参考视频下载。
- `subtitle_extract`：参考视频音频字幕提取。
- `visual_subtitle_extract`：参考视频硬字幕 OCR。
- `auto_clip`：自动剪辑成片。

默认并发：

```json
{
  "baidu_share": 1,
  "douyin_download": 3,
  "subtitle_extract": 1,
  "visual_subtitle_extract": 1,
  "auto_clip": 1
}
```

控制台会自动迁移旧工作间配置到当前逻辑，例如：

- 默认启用 `prefer_funasr_audio_subtitles`。
- 默认启用 `prefer_funasr_sentence_pauses`。
- 自动把主字幕输入切到 `subtitles/audio/*.srt`。
- 自动把视觉字幕作为 `reference_visual_subtitle_glob` 辅助输入。
- 视觉字幕提取默认不跳过旧结果，避免旧缓存影响新逻辑。

## 字幕与 TTS 逻辑

自动剪辑最核心的目标是让成片的解说停顿、字幕分组和参考视频尽量一致。当前逻辑分为几层：

1. Qwen3-ASR + ForcedAligner 优先产出带时间戳的音频字幕。
2. SenseVoice/FunASR 作为可用性回退。
3. 视觉 OCR 字幕辅助纠错、补少量字、优化显示分组。
4. 音频停顿和波形谷值用于校验“该断不断/不该断却断”的边界。
5. AI 只做轻量纠错，保护数字、金额、人名、停顿边界和短对白。
6. 最终 SRT 尽量跟 TTS 实际播放时间线一致，减少一句话被拆到多个字幕块的问题。

Qwen ASR 默认查找路径：

```text
D:\AIModels\venvs\qwen-asr\Scripts\python.exe
D:\AIModels\models\Qwen3-ASR-1.7B
D:\AIModels\models\Qwen3-ForcedAligner-0.6B
%USERPROFILE%\Downloads\Qwen3-ASR-1.7B
%USERPROFILE%\Downloads\Qwen3-ForcedAligner-0.6B
%USERPROFILE%\Desktop\Qwen3-ASR-1.7B
%USERPROFILE%\Desktop\Qwen3-ForcedAligner-0.6B
```

可用环境变量覆盖：

```powershell
$env:SERVER_AUTO_CLIP_QWEN_ASR_PYTHON = "D:\AIModels\venvs\qwen-asr\Scripts\python.exe"
$env:SERVER_AUTO_CLIP_QWEN_ASR_MODEL = "D:\AIModels\models\Qwen3-ASR-1.7B"
$env:SERVER_AUTO_CLIP_QWEN_FORCED_ALIGNER_MODEL = "D:\AIModels\models\Qwen3-ForcedAligner-0.6B"
$env:SERVER_AUTO_CLIP_QWEN_ASR_DEVICE = "cpu"
$env:SERVER_AUTO_CLIP_QWEN_ASR_DTYPE = "bfloat16"
```

`bfloat16` 是当前 Windows 本地测试更稳定的默认 dtype。

## 帧匹配与对白保护

画面重建阶段会从原素材中寻找和参考视频相同或最接近的片段。当前逻辑重点保护三类问题：

- 画面很像但人物台词不同：对白窗口会启用音频内容辅助匹配和 ASR 校验。
- 单个孤帧/跳帧：会在最终阶段做孤立转场帧修复和连续性修复。
- 参考视频倒叙或跨集剪辑：保留参考时间线，不简单强制原素材连续。

输出规格会优先跟随匹配到的原素材，包括分辨率和帧率；参考视频如果是 720x1280，不会因此强制把成片降到 720x1280。

## BGM 匹配逻辑

当前 BGM 已切换为本地库方案，不再依赖联网搜索或平台下载。

默认本地库：

```text
D:\BGM库
runtime\workspaces\<工作间名>\bgm
```

自动匹配流程：

1. 从参考视频中截取音频。
2. 如果 Demucs 可用，先分离参考 BGM，减少人声对白对匹配的干扰。
3. 使用 Chromaprint 判断是否存在同曲或简单变速版本。
4. 如果没有高置信同曲，再使用本地 signature、风格特征和 AudioMuse 风格向量做相似度搜索。
5. 如果常用歌曲与最佳结果接近，会给常用曲一定优先级。
6. 根据参考 BGM 和候选 BGM 的响度估算成片默认音量。

可选工具路径：

```text
C:\Users\24995\Desktop\demucs-main
C:\Users\24995\Desktop\chromaprint-master\build\src\cmd\Release\fpcalc.exe
runtime\tools\chromaprint\fpcalc.exe
```

如果这些工具不可用，软件会自动降级到混合音频和本地特征匹配。

## AI 与 TTS

AI 设置已经是全工作间统一设置，默认：

```json
{
  "ai_api_url": "https://ark.cn-beijing.volces.com/api/v3/chat/completions",
  "ai_model": "doubao-seed-2-0-lite-260215"
}
```

AI 主要用于：

- 修复明显 OCR/ASR 错别字。
- 轻微顺口改写解说。
- 避免大幅改动参考视频本来可用的句子。
- 保护时间窗、数字金额、停顿边界和短对白。

TTS 默认：

```json
{
  "tts_voice": "zh-CN-YunxiNeural",
  "tts_rate": "+8%",
  "enable_backup_tts": false
}
```

可配置 Azure TTS 作为备用 TTS，但默认不启用。

## 输出内容

每次自动剪辑成功后，默认输出到：

```text
runtime\workspaces\<工作间名>\clips
```

通常包含：

- 最终 MP4 成片。
- 最终 SRT 字幕。
- 自动剪辑日志。
- 临时目录中的 job 配置、阶段日志、字幕融合中间产物。

最终 MP4 会尽量做到：

- 音画时间线对齐。
- TTS 与人物对白不互相吞字。
- BGM 避开人声区。
- 新字幕烧录到画面中，并额外保留外部 SRT。

## 重要保护备注

2026-04-21 的最终 MP4 封装修复逻辑必须保留：

- `modules/auto_clip_engine/drama_clone_core.py` 中的最终 MP4 对齐/封装逻辑用于避免音画错位。
- 当前策略会按视频流时长裁剪音频，并在最终参考格式对齐时保留 `-use_editlist 1`。
- 该修复用于避免 `B 帧 + make_zero` 组合造成视频流 `start_time` 非零、音频尾长偏移、浏览器拖动错位等问题。
- 未经用户明确同意，不要修改这部分逻辑，也不要恢复旧的 `-avoid_negative_ts make_zero` 行为。

## 测试

运行核心回归测试：

```powershell
python -m unittest tests.test_subtitle_repair_regressions tests.test_control_center_latest_logic -v
```

这些测试覆盖：

- 音频主导与视觉辅助字幕融合。
- 视觉字幕不能整句污染音频主线。
- 停顿边界不能被本地规则错误合并。
- SRT 尾字补齐与错字纠正。
- 对白窗口音频辅助帧匹配。
- 输出分辨率/帧率选择。
- 控制台旧工作间迁移到最新逻辑。

## 开发约定

- 不把 `runtime/`、`logs/`、模型权重、素材、音频、视频、成片和缓存提交到仓库。
- 新功能优先接入工作间配置和控制台，避免只能命令行使用。
- 字幕、停顿、音色、帧匹配相关修改必须配回归测试。
- 优先从根因修复，尽量少加本地词表和硬编码补丁。
- 修改自动剪辑核心逻辑后，要确认控制台和桌面快捷方式仍然调用同一套最新代码。
