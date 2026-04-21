
const DEFAULT_SETTINGS={ai_api_key:"",ai_api_url:"https://ark.cn-beijing.volces.com/api/v3/chat/completions",ai_model:"doubao-seed-character-251128",ai_fallback_models:[],prefer_funasr_audio_subtitles:true,disable_ai_subtitle_review:false,disable_ai_narration_rewrite:false,prefer_funasr_sentence_pauses:true,force_no_narration_mode:false,narration_background_percent:15,output_watermark_text:"",enable_random_episode_flip:true,random_episode_flip_ratio:.4,enable_random_visual_filter:true,tts_voice:"zh-CN-YunxiNeural",tts_rate:"+8%",enable_backup_tts:false,azure_tts_key:"",azure_tts_region:"",azure_tts_voice:"",clip_output_root:""};
    let latestStatus=null,currentWorkspace="",currentTask=null,selectedWorkspaces=new Set(),activeJobId="",baiduFiles=[],baiduSavedEntries=[],baiduListingUrl="",selectedBaiduKeys=new Set(),baiduExpandedFolders=new Set(),baiduLoginState=null,baiduListingPending=false,toastCounter=0,sidebarCollapsed=false,workspaceFocusTimer=null;
    const baiduOfficialDiagnosisCache=new Map();
    const $=id=>document.getElementById(id);
    const esc=value=>String(value??"").replaceAll("&","&amp;").replaceAll("<","&lt;").replaceAll(">","&gt;").replaceAll('"',"&quot;").replaceAll("'","&#39;");
    const enc=value=>encodeURIComponent(String(value??"")).replaceAll("'","%27");
    const lines=value=>String(value||"").split(/\r?\n/).map(item=>item.trim()).filter(Boolean);
    const one=value=>Array.isArray(value)&&value.length?value[0]:{};
    const asInt=(id,fallback,min=1)=>{const value=Number.parseInt($(id).value,10);return Number.isFinite(value)?Math.max(min,value):fallback};
    const LOG_REPLACEMENTS=[["E:\\鏍风墖","E:\\样片"],["E:\\鎴愮墖","E:\\成片"]];
    const BROKEN_WORKSPACE_NAMES=[];

    function loadSidebarState(){try{sidebarCollapsed=JSON.parse(localStorage.getItem("control_center.sidebarCollapsed")||"false")===true}catch(error){sidebarCollapsed=false}}
    function saveSidebarState(){try{localStorage.setItem("control_center.sidebarCollapsed",JSON.stringify(sidebarCollapsed))}catch(error){}}
    function isDesktopLayout(){return window.innerWidth>1180}
function repairText(value,workspaceName=""){let text=String(value??"");if(!text)return"";for(const [broken,fixed]of LOG_REPLACEMENTS)text=text.split(broken).join(fixed);if(workspaceName){for(const broken of BROKEN_WORKSPACE_NAMES)text=text.split(broken).join(workspaceName)}return text}
function isLikelyBrokenText(value){const text=String(value??"");if(!text)return false;return text.includes("\uFFFD")||text.includes("锟斤拷")||text.includes("\uFFFD\uFFFD")}
function normalizeDisplayText(value,workspaceName=""){const repaired=repairText(value,workspaceName);if(!isLikelyBrokenText(repaired))return repaired;return repaired.replaceAll("\ufffd","").trim()}
    function applySidebarState(){const layout=$("layoutRoot"),toggle=$("sidebarToggleButton"),workspaces=latestStatus?.workspaces||[],jobs=latestStatus?.jobs||[];if(layout)layout.classList.toggle("sidebar-collapsed",sidebarCollapsed&&isDesktopLayout());if(toggle)toggle.textContent=sidebarCollapsed&&isDesktopLayout()?"显示工作间":"隐藏左栏";if($("railWorkspaceCount"))$("railWorkspaceCount").textContent=String(workspaces.length);if($("railRunningCount"))$("railRunningCount").textContent=String(jobs.filter(job=>["running","stopping"].includes(job.status)).length)}
    function toggleSidebar(forceCollapse=null){sidebarCollapsed=typeof forceCollapse==="boolean"?forceCollapse:!sidebarCollapsed;saveSidebarState();applySidebarState()}
    function collapseSidebarAfterRun(){if(!isDesktopLayout())return;sidebarCollapsed=true;saveSidebarState();applySidebarState()}
function focusWorkspaceEditor(){const panel=$("currentWorkspaceName")?.closest(".panel");if(!panel)return;panel.scrollIntoView({behavior:"smooth",block:"start"});panel.classList.remove("panel-emphasis");window.clearTimeout(workspaceFocusTimer);void panel.offsetWidth;panel.classList.add("panel-emphasis");workspaceFocusTimer=window.setTimeout(()=>panel.classList.remove("panel-emphasis"),1100);const focusTarget=$("baiduShareUrl")||panel.querySelector("input,textarea,select,button");if(focusTarget){try{focusTarget.focus({preventScroll:true})}catch(error){focusTarget.focus()}}}
    const STAGE_ORDER=["baidu_share","douyin_download","subtitle_extract","auto_clip"];
    const STAGE_LABELS={baidu_share:"原素材下载",douyin_download:"参考视频下载",subtitle_extract:"字幕提取",auto_clip:"改写与剪辑"};
    function jobWorkspaceMembers(job){const members=Array.isArray(job?.workspace_members)&&job.workspace_members.length?job.workspace_members:[job?.workspace];return members.map(item=>String(item||"").trim()).filter(Boolean)}
    function jobMatchesWorkspace(job,workspaceName){return jobWorkspaceMembers(job).includes(workspaceName)}
    function jobStatusLabel(status){return({running:"运行中",stopping:"停止中",completed:"已完成",failed:"失败",idle:"空闲"})[status]||String(status||"未知")}
    function safeJson(value){try{return JSON.parse(value)}catch(error){return null}}
    function shortText(value,limit=160){const text=String(value||"");return text.length>limit?`${text.slice(0,limit)}...`:text}
    function formatDurationSeconds(value){const total=Math.max(0,Math.floor(Number(value||0)));if(total>=3600){const hours=Math.floor(total/3600),minutes=Math.floor((total%3600)/60);return minutes?`${hours}小时${minutes}分钟`:`${hours}小时`}if(total>=60){const minutes=Math.floor(total/60),seconds=total%60;return seconds?`${minutes}分钟${seconds}秒`:`${minutes}分钟`}return`${total}秒`}
    function joinPreviewList(values,max=6){const list=(Array.isArray(values)?values:[]).map(item=>repairText(item,currentWorkspace)).filter(Boolean);if(!list.length)return"";if(list.length<=max)return list.join("、");return`${list.slice(0,max).join("、")} 等 ${list.length} 个文件`}
    function stageLabel(stageKey){return STAGE_LABELS[stageKey]||repairText(stageKey)}
    function stageKeyFromScope(scope){const raw=String(scope||"").trim().toLowerCase();if(!raw)return"";if(raw==="baidu_share"||raw.startsWith("baidu_official")||raw.startsWith("baidu#")||raw==="baidu")return"baidu_share";if(raw==="douyin_download"||raw.startsWith("douyin#")||raw==="douyin")return"douyin_download";if(raw==="subtitle_extract"||raw.startsWith("subtitle#")||raw==="subtitle")return"subtitle_extract";if(raw==="auto_clip"||raw.startsWith("auto_clip#")||raw.startsWith("clip#")||raw==="clip")return"auto_clip";return""}
    function parsePrefixedJson(text,prefix){if(!String(text||"").startsWith(prefix))return null;return safeJson(String(text||"").slice(prefix.length).trim())}
    function parseStageSummaryMessage(message){const text=String(message||"").trim();const matched=text.match(/^summary total=(\d+) success=(\d+) failed=(\d+) skipped=(\d+)$/);if(matched)return{total:Number(matched[1]),success:Number(matched[2]),failed:Number(matched[3]),skipped:Number(matched[4]),noTasks:false};if(text==="no tasks")return{total:0,success:0,failed:0,skipped:0,noTasks:true};return null}
    function parseLogParts(line,workspaceName=""){const raw=repairText(line,workspaceName);const matched=raw.match(/^(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2},\d{3}) \[([A-Z]+)\](?: \[([^\]]+)\])? (.*)$/);if(!matched)return{timestamp:"",level:"",scope:"",message:raw,raw};return{timestamp:matched[1],level:matched[2],scope:matched[3]||"",message:matched[4],raw}}
    function scopeLabel(scope){
      const raw=String(scope||"").trim();
      const labels={baidu:"百度下载",baidu_official:"百度客户端下载",douyin:"抖音下载",subtitle:"字幕提取",auto_clip:"自动剪辑",clip:"自动剪辑",baidu_share:"原素材下载",douyin_download:"参考视频下载",subtitle_extract:"字幕提取"};
      const matched=raw.match(/^([a-z_]+)#(\d+)(?::(.+))?$/i);
      if(!matched)return labels[raw]||repairText(raw);
      const base=labels[matched[1]]||matched[1];
      const suffix=repairText(matched[3]||"");
      return suffix?`${base} ${matched[2]} · ${shortText(suffix,26)}`:`${base} ${matched[2]}`
    }
    function translateLogMessage(message,workspaceName=""){
      const text=repairText(message,workspaceName),
        workspaceMatched=text.match(/^workspace=(.+?) config=(.+)$/),
        commandMatched=text.match(/^COMMAND (.+)$/),
        loginMatched=text.match(/^LOGIN_STATE (.+)$/),
        mp4Matched=text.match(/^MP4_FILES (.+)$/),
        targetMatched=text.match(/^TARGET (.+)$/),
        resumeMatched=text.match(/^DOWNLOAD_RESUME (.+?) (\d+) (\d+)$/),
        startMatched=text.match(/^DOWNLOAD_START (.+?) (\d+) (\d+) (\d+)$/),
        responseMatched=text.match(/^DOWNLOAD_RESPONSE (.+?) (\d+) (.+)$/),
        progressMatched=text.match(/^DOWNLOAD_PROGRESS (.+?) (\d+)$/),
        doneMatched=text.match(/^DOWNLOAD_DONE (.+?) (\d+)$/),
        downloadedMatched=text.match(/^DOWNLOADED (.+?) (\d+)$/),
        retryMatched=text.match(/^DOWNLOAD_RETRY (.+?) (\d+) (.+)$/),
        failedMatched=text.match(/^failed with exit code (\d+)$/i),
        skipMatched=text.match(/^SKIP (.+)$/),
        stageSummaryMatched=text.match(/^summary total=(\d+) success=(\d+) failed=(\d+) skipped=(\d+)$/),
        crashMatched=text.match(/^crashed before completion: (.+)$/i),
        workspaceCompletedMatched=text.match(/^workspace completed with summaries=(.+)$/),
        workspaceFailedMatched=text.match(/^workspace failed: (.+)$/);
      if(workspaceMatched)return`开始处理工作间 ${repairText(workspaceMatched[1],workspaceName)}，配置文件：${repairText(workspaceMatched[2],workspaceName)}`;
      if(commandMatched){
        const command=repairText(commandMatched[1],workspaceName);
        let title="已启动处理命令";
        if(command.includes("baidu_share_downloader.py")||command.includes("baidu_official_client_handoff.py"))title="已启动百度网盘下载";
        else if(command.toLowerCase().includes("douyin"))title="已启动抖音下载";
        else if(command.includes("subtitle_batch_runner.py"))title="已启动字幕提取";
        else if(command.includes("auto_clip_engine"))title="已启动自动剪辑";
        else if(command.includes("batch_runner.py"))title="已启动批处理";
        return`${title}：${command}`;
      }
      if(loginMatched){
        const data=safeJson(loginMatched[1]);
        if(data)return`百度登录状态：${data.logged_in?"已登录":"未登录"}${data.profile_in_use?"，登录窗口还未关闭":""}`;
        return"已读取百度登录状态";
      }
      if(mp4Matched){
        const data=safeJson(mp4Matched[1]);
        return Array.isArray(data)?`已从网盘链接中提取到 ${data.length} 个可下载视频`:"已提取网盘视频列表";
      }
      if(targetMatched){
        const data=safeJson(targetMatched[1]);
        const filename=data?.server_filename||data?.path||"";
        return filename?`已锁定本次下载目标：${repairText(filename,workspaceName)}`:"已锁定本次下载目标";
      }
      if(text.startsWith("RUNTIME "))return"网盘运行参数已准备完成";
      if(text.startsWith("USER_AGENT "))return"浏览器环境已准备完成";
      if(text.startsWith("SHARE_SHORT_ID "))return`分享短链 ID：${text.slice("SHARE_SHORT_ID ".length).trim()}`;
      if(text.startsWith("SIGN "))return"已拿到下载签名参数";
      if(text.startsWith("SHAREDOWNLOAD_REQUESTS "))return"已请求分享下载数据";
      if(text.startsWith("SHAREDOWNLOAD_BROWSER "))return"浏览器校验通过，正在获取下载数据";
      if(text.startsWith("SHAREDOWNLOAD_NEEDS_TRANSFER "))return"该分享需要先转存到你的网盘空间";
      if(text.startsWith("ACCESS_TOKEN "))return"已获取下载令牌";
      if(text.startsWith("TRANSFER "))return"已完成转存，准备开始下载";
      if(text.startsWith("OWN_FILE "))return"已确认转存后的网盘文件";
      if(text.startsWith("OFFICIAL_CLIENT_DETECTED"))return"已检测到官方百度网盘客户端";
      const officialLogin=parsePrefixedJson(text,"OFFICIAL_CLIENT_LOGIN");
      if(officialLogin)return`百度专用登录检查完成：${officialLogin.logged_in?"已登录":"未登录"}。`;
      const officialTargets=parsePrefixedJson(text,"OFFICIAL_CLIENT_TARGETS");
      if(officialTargets)return`已从分享里锁定 ${officialTargets.count||0} 个要交给官方客户端下载的视频。`;
      const officialService=parsePrefixedJson(text,"OFFICIAL_CLIENT_SERVICE");
      if(officialService)return`官方客户端本地服务已就绪，客户端版本 ${repairText(officialService.client_version||"-",workspaceName)}。`;
      const officialAccount=parsePrefixedJson(text,"OFFICIAL_CLIENT_ACCOUNT");
      if(officialAccount)return"已确认当前百度账号身份，准备交给官方客户端下载。";
      const officialShareDownload=parsePrefixedJson(text,"OFFICIAL_CLIENT_SHAREDOWNLOAD");
      if(officialShareDownload)return`已向百度网盘请求 ${officialShareDownload.file_count||0} 个文件的客户端下载数据。`;
      const officialInvoker=parsePrefixedJson(text,"OFFICIAL_CLIENT_INVOKER");
      if(officialInvoker)return"已向官方客户端发送下载接管指令。";
      const officialMovePlan=parsePrefixedJson(text,"OFFICIAL_CLIENT_MOVE_PLAN");
      if(officialMovePlan)return`已开始监控客户端下载目录，待搬运 ${officialMovePlan.pending_count||0} 个文件，当前检查目录 ${officialMovePlan.search_roots?.length||officialMovePlan.search_root_count||0} 个。`;
      const officialMoveReady=parsePrefixedJson(text,"OFFICIAL_CLIENT_MOVE_READY");
      if(officialMoveReady)return`目标文件已在工作间目录就位：${repairText(officialMoveReady.label||officialMoveReady.target_path||"",workspaceName)}`;
      const officialMoved=parsePrefixedJson(text,"OFFICIAL_CLIENT_MOVED");
      if(officialMoved)return`已搬运到工作间：${repairText(officialMoved.label||officialMoved.target_path||"",workspaceName)}`;
      const officialUiConfirm=parsePrefixedJson(text,"OFFICIAL_CLIENT_UI_CONFIRM");
      if(officialUiConfirm)return`已自动确认官方客户端窗口：${repairText(officialUiConfirm.button_text||officialUiConfirm.action||"确定",workspaceName)}`;
      const officialUiConfirmSkip=parsePrefixedJson(text,"OFFICIAL_CLIENT_UI_CONFIRM_SKIP");
      if(officialUiConfirmSkip)return"官方客户端没有弹出需要手动确认的窗口，继续等待下载完成。";
      const officialWait=parsePrefixedJson(text,"OFFICIAL_CLIENT_WAIT");
      if(officialWait){
        const pendingCount=Number(officialWait.pending_count||0);
        const labels=joinPreviewList(officialWait.pending_labels||[],4);
        const waited=formatDurationSeconds(Number(officialWait.wait_round||0)*6);
        const blockedHint=Number(officialWait.wait_round||0)*6>=900?" 这一步已经明显卡住，请检查官方客户端是否还在下载，或下载目录是否被改动。":"";
        return`官方客户端下载后仍在等待 ${pendingCount} 个文件下载并搬运${labels?`：${labels}`:""}。已等待约 ${waited}，当前正在检查 ${officialWait.search_root_count||0} 个下载目录。${blockedHint}`;
      }
      const officialQueued=parsePrefixedJson(text,"OFFICIAL_CLIENT_QUEUED");
      if(officialQueued)return`官方客户端阶段已完成：共接管 ${officialQueued.queued_count||0} 个文件，已搬运 ${officialQueued.moved_count||0} 个。`;
      if(text.startsWith("OFFICIAL_CLIENT_MOVE_SKIP"))return"这次没有可自动搬运的目标路径，已跳过搬运等待。";
      if(text.startsWith("OFFICIAL_CLIENT_NOTE"))return`说明：${repairText(text.slice("OFFICIAL_CLIENT_NOTE".length).trim(),workspaceName)}`;
      if(text.startsWith("HANDOFF "))return`已把下载任务交给官方百度网盘客户端：${repairText(text.slice("HANDOFF ".length),workspaceName)}`;
      if(text.startsWith("NOTE "))return`说明：${repairText(text.slice("NOTE ".length),workspaceName)}`;
      if(resumeMatched)return`继续下载 ${repairText(resumeMatched[1],workspaceName)}，从 ${formatSize(resumeMatched[2])} 开始，线程 ${resumeMatched[3]}`;
      if(startMatched)return`开始下载 ${repairText(startMatched[1],workspaceName)}，总大小 ${formatSize(startMatched[2])}，线程 ${startMatched[3]}${Number(startMatched[4])>0?`，断点续传起点 ${formatSize(startMatched[4])}`:""}`;
      if(responseMatched)return`下载响应 ${repairText(responseMatched[1],workspaceName)}，状态 ${responseMatched[2]}，${repairText(responseMatched[3],workspaceName)}`;
      if(progressMatched)return`正在下载 ${repairText(progressMatched[1],workspaceName)}，已下载 ${formatSize(progressMatched[2])}`;
      if(doneMatched)return`下载完成 ${repairText(doneMatched[1],workspaceName)}，总大小 ${formatSize(doneMatched[2])}`;
      if(downloadedMatched)return`已保存到 ${repairText(downloadedMatched[1],workspaceName)}（${formatSize(downloadedMatched[2])}）`;
      if(retryMatched)return`${repairText(retryMatched[1],workspaceName)} 下载重试第 ${retryMatched[2]} 次：${repairText(retryMatched[3],workspaceName)}`;
      if(skipMatched)return`已跳过：${repairText(skipMatched[1],workspaceName)}`;
      if(stageSummaryMatched)return`本阶段完成：共 ${stageSummaryMatched[1]} 项，成功 ${stageSummaryMatched[2]} 项，失败 ${stageSummaryMatched[3]} 项，跳过 ${stageSummaryMatched[4]} 项。`;
      if(text==="no tasks")return"本阶段没有需要处理的任务。";
      if(text==="finished successfully")return"任务已成功完成";
      if(failedMatched)return`任务失败，退出码 ${failedMatched[1]}`;
      if(crashMatched)return`任务异常中断：${repairText(crashMatched[1],workspaceName)}`;
      if(workspaceCompletedMatched)return`工作间全部阶段已处理完成：${repairText(workspaceCompletedMatched[1],workspaceName)}`;
      if(workspaceFailedMatched)return`工作间处理失败：${repairText(workspaceFailedMatched[1],workspaceName)}`;
      if(text==="Traceback (most recent call last):"||text==="Traceback")return"程序抛出异常，下面开始输出错误堆栈";
      return text;
    }
    function localizeLogLine(line,job=null){const workspaceName=jobWorkspaceMembers(job||{}).find(Boolean)||currentWorkspace||"";const parts=parseLogParts(line,workspaceName);if(!parts.timestamp)return translateLogMessage(parts.message,workspaceName);const result=[parts.timestamp];if(parts.scope){const scopeText=scopeLabel(parts.scope);if(scopeText)result.push(`[${scopeText}]`)}if(parts.level==="ERROR")result.push("[错误]");if(parts.level==="WARNING"||parts.level==="WARN")result.push("[警告]");return`${result.join(" ")} ${translateLogMessage(parts.message,workspaceName)}`}
    function localizedJobLines(job){return(job?.recent_lines||[]).map(line=>localizeLogLine(line,job))}
    function latestJobPreview(job){const lines=localizedJobLines(job).filter(Boolean);return lines.length?shortText(lines[lines.length-1],160):"等待输出..."}
    function emptyProgressState(status="idle"){return{status,percent:status==="completed"?100:0,totalStages:0,stageIndex:0,stageKey:"",stageLabel:"",stepText:"",detail:"",note:"",blocked:false}}
    function formatJobMeta(job){const members=jobWorkspaceMembers(job),head=members.length>1?`批量工作间 ${members.length} 个`:`工作间 ${repairText(members[0]||job.workspace||"-",members[0]||currentWorkspace)}`;return`${head} | 进程 ${repairText(job.pid||"-")} | ${formatTime(job.started_at)}`}
    function analyzeJobProgress(job,workspaceName=""){
      const empty=emptyProgressState(job?.status||"idle");
      if(!job)return empty;
      const workspace=(latestStatus?.workspaces||[]).find(item=>item.name===workspaceName)||{};
      const taskSummary=workspace.task_summary||{};
      const rawLines=Array.isArray(job.recent_lines)?job.recent_lines:[];
      const stageSignals=new Set();
      const stageSummaries={};
      const latestStageMessages={};
      const finalStatuses=new Map();
      let latestStageKey="";
      let latestMessage="";
      let waitInfo=null;

      for(const line of rawLines){
        const parts=parseLogParts(line,workspaceName);
        const stageKey=stageKeyFromScope(parts.scope);
        if(stageKey){
          stageSignals.add(stageKey);
          latestStageKey=stageKey;
        }
        const translated=translateLogMessage(parts.message,workspaceName);
        if(translated){
          latestMessage=translated;
          if(stageKey)latestStageMessages[stageKey]=translated;
        }
        const summaryData=parseStageSummaryMessage(parts.message);
        if(summaryData&&stageKey)stageSummaries[stageKey]=summaryData;
        if(stageKey&&parts.scope){
          let finalStatus="";
          if(parts.message==="finished successfully")finalStatus="success";
          else if(String(parts.message||"").startsWith("SKIP "))finalStatus="skipped";
          else if(/failed with exit code/i.test(parts.message))finalStatus="failed";
          if(finalStatus)finalStatuses.set(parts.scope,{stageKey,status:finalStatus});
        }
        const officialWait=parsePrefixedJson(parts.message,"OFFICIAL_CLIENT_WAIT");
        if(officialWait){
          waitInfo={
            pendingCount:Number(officialWait.pending_count||0),
            pendingLabels:Array.isArray(officialWait.pending_labels)?officialWait.pending_labels:[],
            waitRound:Number(officialWait.wait_round||0),
            searchRootCount:Number(officialWait.search_root_count||0),
          };
          latestStageKey="baidu_share";
          stageSignals.add("baidu_share");
        }
      }

      let configuredStages=STAGE_ORDER.filter(stageKey=>Number(taskSummary?.[stageKey]||0)>0);
      if(!configuredStages.length)configuredStages=STAGE_ORDER.filter(stageKey=>stageSignals.has(stageKey));
      if(!configuredStages.length)return empty;

      const stageCounts=Object.fromEntries(STAGE_ORDER.map(stageKey=>[stageKey,{success:0,skipped:0,failed:0}]));
      for(const item of finalStatuses.values()){
        if(stageCounts[item.stageKey])stageCounts[item.stageKey][item.status]+=1;
      }

      function stageMetrics(stageKey){
        const configuredTotal=Number(taskSummary?.[stageKey]||0);
        const counts=stageCounts[stageKey]||{success:0,skipped:0,failed:0};
        const summaryData=stageSummaries[stageKey]||null;
        let total=summaryData?Number(summaryData.total||0):configuredTotal;
        let done=summaryData?Number(summaryData.success||0)+Number(summaryData.failed||0)+Number(summaryData.skipped||0):counts.success+counts.failed+counts.skipped;
        if(stageKey==="baidu_share"&&waitInfo&&configuredTotal>0){
          total=configuredTotal;
          done=Math.max(done,Math.max(0,configuredTotal-waitInfo.pendingCount));
        }
        const complete=job.status==="completed"||!!summaryData||(total>0&&done>=total);
        return{total,done,complete};
      }

      let currentStage=configuredStages.find(stageKey=>!stageMetrics(stageKey).complete)||configuredStages[configuredStages.length-1];
      if(job.status!=="completed"&&latestStageKey&&configuredStages.includes(latestStageKey)&&!stageMetrics(latestStageKey).complete)currentStage=latestStageKey;
      const currentIndex=Math.max(0,configuredStages.indexOf(currentStage));
      const metrics=stageMetrics(currentStage);
      let completedBefore=0;
      for(let index=0;index<currentIndex;index+=1){
        if(stageMetrics(configuredStages[index]).complete)completedBefore+=1;
      }
      const currentRatio=job.status==="completed"?1:(metrics.total>0?Math.min(1,metrics.done/metrics.total):(stageMetrics(currentStage).complete?1:0));
      const percent=Math.max(0,Math.min(100,Math.round(job.status==="completed"?100:((completedBefore+currentRatio)/configuredStages.length)*100)));
      const stepText=`第 ${currentIndex+1}/${configuredStages.length} 步：${stageLabel(currentStage)}`;
      const detail=metrics.total>0?`${stepText} · 已完成 ${metrics.done}/${metrics.total}`:stepText;
      const waitSeconds=waitInfo?waitInfo.waitRound*6:0;
      const blocked=Boolean(waitInfo&&waitInfo.pendingCount>0&&waitSeconds>=900);
      let note=latestStageMessages[currentStage]||latestMessage||"";
      if(waitInfo&&currentStage==="baidu_share"&&waitInfo.pendingCount>0){
        note=`百度网盘官方客户端仍有 ${waitInfo.pendingCount} 个文件没完成下载并搬运：${joinPreviewList(waitInfo.pendingLabels,6)||"待确认文件"}。已等待约 ${formatDurationSeconds(waitSeconds)}，正在检查 ${waitInfo.searchRootCount} 个下载目录。${blocked?" 这一步已经明显卡住，请检查官方客户端是否仍在下载，或下载目录是否被改动。":""}`;
      }
      return{
        status:job.status||"idle",
        percent,
        totalStages:configuredStages.length,
        stageIndex:currentIndex+1,
        stageKey:currentStage,
        stageLabel:stageLabel(currentStage),
        stepText,
        detail,
        note,
        blocked,
      };
    }
    function renderProgressMarkup(progress,compact=false){
      if(!progress||!progress.totalStages)return"";
      const tone=progress.blocked?"warn":(progress.status==="failed"?"bad":(progress.status==="completed"?"good":"run"));
      return`<div class="progress-wrap ${compact?"compact":""} ${tone}"><div class="progress-head"><strong>${esc(progress.stepText||"处理中")}</strong><span>${esc(`${progress.percent}%`)}</span></div><div class="progress-bar"><div class="progress-fill ${tone}" style="width:${Math.max(0,Math.min(100,Number(progress.percent)||0))}%"></div></div><div class="progress-note">${esc(progress.detail||"")}${progress.note?`<br>${esc(progress.note)}`:""}</div></div>`;
    }
    function runtimeForWorkspace(workspaceName){
      const jobs=(latestStatus?.jobs||[]).filter(job=>jobMatchesWorkspace(job,workspaceName));
      const activeJobs=jobs.filter(job=>["running","stopping"].includes(job.status));
      const latestJob=jobs[0]||null;
      if(activeJobs.length){
        const status=activeJobs.some(job=>job.status==="stopping")?"stopping":"running";
        const lead=activeJobs[0];
        const progress=analyzeJobProgress(lead,workspaceName);
        const className=progress.blocked?"warn":statusClass(status);
        return{
          status:progress.blocked?"waiting":status,
          className,
          label:progress.blocked?"等待下载":"运行中",
          badge:`${progress.percent}%`,
          detail:activeJobs.length>1?`当前有 ${activeJobs.length} 个相关任务正在处理。`:progress.detail||`${formatTime(lead.started_at)} 已启动`,
          preview:progress.note||latestJobPreview(lead),
          progress,
          job:lead,
        };
      }
      if(latestJob){
        const progress=analyzeJobProgress(latestJob,workspaceName);
        return{
          status:latestJob.status||"idle",
          className:statusClass(latestJob.status),
          label:jobStatusLabel(latestJob.status),
          badge:progress.totalStages?`${progress.percent}%`:jobStatusLabel(latestJob.status),
          detail:progress.detail||`${formatTime(latestJob.started_at)} 最近一次运行`,
          preview:progress.note||latestJobPreview(latestJob),
          progress,
          job:latestJob,
        };
      }
      return{status:"idle",className:"idle",label:"空闲",badge:"未启动",detail:"还没有启动记录。",preview:"等待启动后显示实时摘要。",progress:emptyProgressState(),job:null};
    }

    function baseTask(name){return{workspace_name:name||"",concurrency:{baidu_share:1,douyin_download:3,subtitle_extract:1,auto_clip:1},settings:{...DEFAULT_SETTINGS},baidu_share:[],douyin_download:[],subtitle_extract:[],auto_clip:[{reference_video_glob:"downloads/douyin/*",reference_subtitle_glob:"",source_dir:"downloads/baidu",output_subdir:"clips",temp_subdir:"temp/auto_clip",title:"{workspace_name}_{reference_stem}",match_all_references:true,skip_existing:true,keep_temp:false}]}}
    function normalizeTask(task,name){const base=baseTask(name),source=task||{},merged={...base,...source};merged.workspace_name=name||merged.workspace_name||"";merged.concurrency={...base.concurrency,...(source.concurrency||{})};merged.settings={...base.settings,...(source.settings||{})};if(!Array.isArray(merged.settings.ai_fallback_models))merged.settings.ai_fallback_models=[];merged.baidu_share=Array.isArray(merged.baidu_share)?merged.baidu_share:[];merged.douyin_download=Array.isArray(merged.douyin_download)?merged.douyin_download:[];merged.subtitle_extract=Array.isArray(merged.subtitle_extract)?merged.subtitle_extract:[];merged.auto_clip=Array.isArray(merged.auto_clip)&&merged.auto_clip.length?merged.auto_clip:base.auto_clip;return merged}
    function setValue(id,value){$(id).value=value??""}function setChecked(id,value){$(id).checked=!!value}
async function api(path,options={}){const headers={...(options.headers||{})};if(options.body&&!headers["Content-Type"])headers["Content-Type"]="application/json";const response=await fetch(path,{cache:options.cache||"no-store",...options,headers});const contentType=response.headers.get("content-type")||"";const payload=contentType.includes("application/json")?await response.json():{};if(!response.ok)throw new Error(payload.error||`请求失败：${response.status}`);return payload}
    async function withBusy(button,callback){if(!button)return await callback();button.disabled=true;button.classList.add("loading");try{return await callback()}finally{button.disabled=false;button.classList.remove("loading")}}
    function toast(message,type="info"){const id=`toast_${++toastCounter}`;$("toastHost").insertAdjacentHTML("beforeend",`<div id="${id}" class="toast ${esc(type)}">${esc(message)}</div>`);setTimeout(()=>{const node=$(id);if(node)node.remove()},type==="error"?6200:3600)}
    function formatSize(size){const value=Number(size||0);if(!Number.isFinite(value)||value<=0)return"未知大小";if(value>=1073741824)return`${(value/1073741824).toFixed(2)} GB`;if(value>=1048576)return`${(value/1048576).toFixed(1)} MB`;if(value>=1024)return`${(value/1024).toFixed(1)} KB`;return`${value} B`}
    function getDouyinLinkInputs(){return Array.from(document.querySelectorAll("[data-douyin-link]"))}
    function stripDouyinShareTail(value){let text=String(value||"").trim();while(text&&`"'<>)]}，。！？；：,.;!?`.includes(text.slice(-1)))text=text.slice(0,-1).trim();return text}
    function extractDouyinShareUrl(value){const raw=String(value||"").trim();if(!raw)return"";const direct=raw.match(/https?:\/\/[^\s]+/i);if(direct)return stripDouyinShareTail(direct[0]);const host=raw.match(/(?:(?:v\.douyin\.com|www\.douyin\.com|douyin\.com|vm\.tiktok\.com|www\.tiktok\.com|m\.tiktok\.com|b23\.tv|www\.bilibili\.com)\/[^\s]+)/i);if(host)return stripDouyinShareTail(`https://${host[0]}`);return raw}
    function normalizeDouyinLinkInput(input){if(!input)return;const normalized=extractDouyinShareUrl(input.value);if(normalized&&normalized!==input.value.trim())input.value=normalized;updateDouyinCount()}
    function getDouyinLinks(){return getDouyinLinkInputs().map(input=>extractDouyinShareUrl(input.value)).filter(Boolean)}
    function updateDouyinCount(){$("douyinCount").textContent=`${getDouyinLinks().length} 条`}
    function addDouyinLinkRow(value=""){const row=document.createElement("div");row.className="link-row";row.innerHTML=`<input type="text" data-douyin-link placeholder="可直接粘贴整段抖音分享文案或纯链接" value="${esc(value)}" oninput="updateDouyinCount()" onblur="normalizeDouyinLinkInput(this)"><button class="secondary" type="button" onclick="removeDouyinLinkRow(this)">删除</button>`;$("douyinLinkList").appendChild(row);updateDouyinCount();return row}
    function removeDouyinLinkRow(button){const rows=Array.from($("douyinLinkList").querySelectorAll(".link-row"));if(rows.length<=1){const input=rows[0]?.querySelector("[data-douyin-link]");if(input)input.value="";updateDouyinCount();return}button.closest(".link-row")?.remove();updateDouyinCount()}
    function setDouyinLinks(values){const list=$("douyinLinkList");list.innerHTML="";const normalized=(values||[]).map(value=>String(value||"").trim()).filter(Boolean);for(const value of (normalized.length?normalized:[""]))addDouyinLinkRow(value);updateDouyinCount()}
    function clearDouyinLinks(){setDouyinLinks([])}
function baiduFileKey(file){const fsid=String(file.fs_id??"").trim();if(fsid)return`fsid:${fsid}`;const path=String(file.path??"").trim();if(path)return`path:${path}`;const name=String(file.name||file.server_filename||"").trim();return`name:${name}`}
function baiduPathSegments(file){const fallbackName=normalizeDisplayText(file.name||file.server_filename||"",currentWorkspace).trim();const path=normalizeDisplayText(file.path??"",currentWorkspace).trim();if(!path||isLikelyBrokenText(path))return[fallbackName].filter(Boolean);const parts=path.split("/").filter(Boolean).map(part=>normalizeDisplayText(part,currentWorkspace).trim()).filter(Boolean);return(parts.length>1?parts.slice(1):parts).filter(Boolean)}
function createBaiduTreeNode(name,id){return{name,id,folders:new Map(),files:[]}}
function buildBaiduTree(files){const root=createBaiduTreeNode("root","root");for(const file of files){const parts=baiduPathSegments(file);if(!parts.length)continue;let cursor=root;for(let index=0;index<parts.length;index+=1){const part=parts[index],isFile=index===parts.length-1;if(isFile){cursor.files.push({...file,_treeName:part});continue}const folderId=`${cursor.id}/${part}`;if(!cursor.folders.has(part))cursor.folders.set(part,createBaiduTreeNode(part,folderId));cursor=cursor.folders.get(part)}}return root}
function collectDefaultOpenBaiduFolders(node,depth=0,maxDepth=1,bucket=new Set()){for(const folder of node.folders.values()){if(depth<=maxDepth)bucket.add(folder.id);collectDefaultOpenBaiduFolders(folder,depth+1,maxDepth,bucket)}return bucket}
function baiduFolderVideoCount(node){let total=node.files.length;for(const child of node.folders.values())total+=baiduFolderVideoCount(child);return total}
function baiduFolderSelectedCount(node){let total=0;for(const file of node.files){if(selectedBaiduKeys.has(baiduFileKey(file)))total+=1}for(const child of node.folders.values())total+=baiduFolderSelectedCount(child);return total}
function findBaiduFolderNode(node,folderId){if(!node)return null;if(node.id===folderId)return node;for(const child of node.folders.values()){const found=findBaiduFolderNode(child,folderId);if(found)return found}return null}
function collectBaiduNodeFiles(node,bucket=[]){if(!node)return bucket;for(const file of node.files)bucket.push(file);for(const child of node.folders.values())collectBaiduNodeFiles(child,bucket);return bucket}
function baiduTreeSource(){return baiduFiles.length?baiduFiles:baiduSavedEntries}
function withFolderActionEvent(event){if(event){event.preventDefault();event.stopPropagation()}}
function applyBaiduFolderSelection(folderId,checked){const root=buildBaiduTree(baiduTreeSource()),folder=findBaiduFolderNode(root,folderId);if(!folder)return;for(const file of collectBaiduNodeFiles(folder)){const key=baiduFileKey(file);if(checked)selectedBaiduKeys.add(key);else selectedBaiduKeys.delete(key)}renderBaiduFiles()}
function selectBaiduFolder(event,folderId){withFolderActionEvent(event);applyBaiduFolderSelection(folderId,true)}
function clearBaiduFolder(event,folderId){withFolderActionEvent(event);applyBaiduFolderSelection(folderId,false)}
function renderBaiduFileNode(file){const key=baiduFileKey(file),name=normalizeDisplayText(file._treeName||file.name||file.server_filename||"",currentWorkspace).trim();if(!name)return"";return`<label class="file-row baidu-tree-file"><input type="checkbox" data-key="${esc(key)}" ${selectedBaiduKeys.has(key)?"checked":""} onchange="toggleBaiduFile(this)"><span><strong>${esc(name)}</strong><small>${esc(formatSize(file.size))}</small></span><span class="pill">视频</span></label>`}
function renderBaiduTreeChildren(node,depth=0){const folders=Array.from(node.folders.values()).sort((a,b)=>normalizeDisplayText(a.name,currentWorkspace).localeCompare(normalizeDisplayText(b.name,currentWorkspace),"zh-CN"));const files=[...node.files].sort((a,b)=>normalizeDisplayText(a._treeName||a.name||"",currentWorkspace).localeCompare(normalizeDisplayText(b._treeName||b.name||"",currentWorkspace),"zh-CN"));const folderHtml=folders.map(folder=>{const open=baiduExpandedFolders.has(folder.id),folderName=normalizeDisplayText(folder.name,currentWorkspace),totalCount=baiduFolderVideoCount(folder),selectedCount=baiduFolderSelectedCount(folder);return`<details class="baidu-folder" data-folder-id="${esc(folder.id)}" ${open?"open":""} ontoggle="toggleBaiduFolder(this)"><summary class="baidu-folder-summary"><span class="baidu-folder-name">${esc(folderName)}</span><span class="baidu-folder-meta"><span class="baidu-folder-status">${selectedCount}/${totalCount} 已选</span><span class="pill">${totalCount} 个视频</span><span class="baidu-folder-tools"><button class="secondary" type="button" onclick="selectBaiduFolder(event,'${esc(folder.id)}')" title="一键选中这个文件夹里的全部视频">选中文件夹内容</button><button class="secondary" type="button" onclick="clearBaiduFolder(event,'${esc(folder.id)}')" title="清空这个文件夹里的已选视频">取消文件夹内容</button></span></span></summary><div class="baidu-folder-children">${renderBaiduTreeChildren(folder,depth+1)}</div></details>`}).join("");const fileHtml=files.map(renderBaiduFileNode).join("");return`${folderHtml}${fileHtml}`}
    function renderBaiduLoginState(){const node=$("baiduLoginStatus");if(!node)return;if(!baiduLoginState){node.className="pill warn";node.textContent="百度未检查";return}if(baiduLoginState.profile_in_use){node.className="pill warn";node.textContent="登录窗口未关闭";return}if(baiduLoginState.logged_in){node.className="pill good";node.textContent="百度已登录";return}node.className="pill warn";node.textContent="百度未登录"}
    function renderBaiduFiles(){const picker=$("baiduFilePicker"),shareUrl=$("baiduShareUrl").value.trim();$("baiduSelectedCount").textContent=`${selectedBaiduKeys.size} 个`;renderBaiduLoginState();if(!shareUrl){picker.innerHTML=`<div class="empty">输入网盘链接后点击“提取内容”。</div>`;return}if(baiduListingPending){picker.innerHTML=`<div class="empty">正在重新提取网盘目录，请稍等。旧缓存已经隐藏，避免继续显示乱码或不完整的文件树。</div>`;return}if(!baiduFiles.length){if(baiduSavedEntries.length){const preview=baiduSavedEntries.slice(0,6).map(item=>repairText(item.name||item.target_filename||"",currentWorkspace)).filter(Boolean).join("、");picker.innerHTML=`<div class="empty">当前只有已保存的下载草稿，共 ${baiduSavedEntries.length} 项。为了显示完整中文目录和折叠文件夹，请点上面的“提取内容”重新读取网盘目录。${preview?`<div class="hint" style="margin-top:8px">已保存示例：${esc(preview)}</div>`:""}</div>`;return}picker.innerHTML=`<div class="empty">提取后会按文件夹折叠显示，展开文件夹后可以逐个勾选，也可以点右侧按钮一键选中文件夹内容。</div>`;return}picker.innerHTML=renderBaiduTreeChildren(buildBaiduTree(baiduFiles))}
    function toggleBaiduFolder(node){const folderId=node.dataset.folderId;if(!folderId)return;if(node.open)baiduExpandedFolders.add(folderId);else baiduExpandedFolders.delete(folderId)}
    function toggleBaiduFile(input){const key=input.dataset.key;if(!key)return;input.checked?selectedBaiduKeys.add(key):selectedBaiduKeys.delete(key);renderBaiduFiles()}
    function selectAllBaiduFiles(){for(const file of (baiduFiles.length?baiduFiles:baiduSavedEntries))selectedBaiduKeys.add(baiduFileKey(file));renderBaiduFiles()}
    function clearBaiduSelection(){selectedBaiduKeys.clear();renderBaiduFiles()}
    function openLocalFilePicker(inputId){if(!currentWorkspace){toast("请先选择工作间。","error");return}const input=$(inputId);if(!input)return;input.value="";input.click()}
    async function uploadLocalFiles(input,kind){const files=Array.from(input?.files||[]);if(!files.length)return;if(!currentWorkspace){toast("请先选择工作间。","error");input.value="";return}const kindLabels={source:"原素材",reference:"参考视频"},kindLabel=kindLabels[kind]||"文件";try{for(const file of files){const response=await fetch(`/api/workspaces/${encodeURIComponent(currentWorkspace)}/upload-file`,{method:"POST",headers:{"Content-Type":"application/octet-stream","X-Upload-Kind":kind,"X-Upload-Name":encodeURIComponent(file.name||"upload.bin")},body:file});const payload=await response.json().catch(()=>({}));if(!response.ok)throw new Error(payload.error||`上传失败：${file.name}`)}await loadStatus(true);toast(`已上传 ${files.length} 个${kindLabel}。`,"success")}catch(error){toast(error.message||String(error),"error")}finally{input.value=""}}
    function workspaceAssetSummary(name=currentWorkspace){return(latestStatus?.workspaces||[]).find(item=>item.name===name)?.asset_summary||{}}
    function assetSectionCount(section){return Number(section?.count||0)}
    function assetSectionRecent(section){return(Array.isArray(section?.recent)?section.recent:[]).map(item=>repairText(item,currentWorkspace)).filter(Boolean)}
    function assetSectionItems(section){return Array.isArray(section?.items)?section.items:[]}
    function formatImportStatus(label,section,emptyText){const count=assetSectionCount(section);if(!count)return emptyText;const recent=assetSectionRecent(section);return recent.length?`当前工作间已导入 ${count} 个${label}，最近：${recent.join("、")}`:`当前工作间已导入 ${count} 个${label}。`}
    function formatBytes(value){const size=Number(value||0);if(!Number.isFinite(size)||size<=0)return"";if(size<1024)return`${size} B`;if(size<1024**2)return`${(size/1024).toFixed(size>=10*1024?0:1)} KB`;if(size<1024**3)return`${(size/1024**2).toFixed(size>=10*1024**2?0:1)} MB`;return`${(size/1024**3).toFixed(1)} GB`}
    function renderWorkspaceAssetPicker(nodeId,section,emptyText){const node=$(nodeId);if(!node)return;const items=assetSectionItems(section);if(!items.length){node.innerHTML=`<div class="empty">${esc(emptyText)}</div>`;return}const hiddenCount=Math.max(0,Number(section?.hidden_count||0));node.innerHTML=`${items.map(item=>{const relative=normalizeDisplayText(item?.relative_path||item?.name||"",currentWorkspace),name=normalizeDisplayText(item?.name||relative.split(/[\\/]/).pop()||"",currentWorkspace),sizeText=formatBytes(item?.size),meta=relative&&relative!==name?relative:"已在当前工作间";return`<div class="file-row"><span class="pill good">文件</span><div><strong title="${esc(name)}">${esc(name)}</strong><small title="${esc(relative||meta)}">${esc(meta)}</small></div><span class="pill">${esc(sizeText||"已导入")}</span></div>`}).join("")}${hiddenCount?`<div class="hint">还有 ${hiddenCount} 个文件未展开显示。</div>`:""}`}
    function renderImportStatuses(){const sourceNode=$("sourceImportStatus"),referenceNode=$("referenceImportStatus");if(!currentWorkspace){if(sourceNode)sourceNode.textContent="请先选择工作间后再导入原素材。";if(referenceNode)referenceNode.textContent="请先选择工作间后再导入参考视频。";renderWorkspaceAssetPicker("sourceAssetList",null,"请先选择工作间后查看原素材。");renderWorkspaceAssetPicker("referenceAssetList",null,"请先选择工作间后查看参考视频。");return}const assets=workspaceAssetSummary(currentWorkspace);if(sourceNode)sourceNode.textContent=formatImportStatus("原素材",assets.source,"当前工作间还没有导入原素材，可直接点击上方按钮浏览导入。");if(referenceNode)referenceNode.textContent=formatImportStatus("参考视频",assets.reference,"当前工作间还没有导入参考视频，可直接点击上方按钮浏览导入。");renderWorkspaceAssetPicker("sourceAssetList",assets.source,"当前工作间还没有原素材视频。");renderWorkspaceAssetPicker("referenceAssetList",assets.reference,"当前工作间还没有参考视频。")}
    function joinOutputPath(root,name){const trimmed=String(root||"").trim();if(!trimmed)return"";if(/[\\/]$/.test(trimmed))return`${trimmed}${name}`;return`${trimmed}${trimmed.includes("/")?"/":"\\"}${name}`}
    function inferClipOutputRoot(outputSubdir,workspaceName){const value=String(outputSubdir||"").trim();if(!value||value==="clips")return"";const normalized=value.replace(/[\\/]+$/,"");const parts=normalized.split(/[\\/]/).filter(Boolean);if(parts.length&&parts[parts.length-1]===workspaceName){const index=Math.max(normalized.lastIndexOf("\\"),normalized.lastIndexOf("/"));return index>=0?normalized.slice(0,index):""}return normalized}
    function normalizeFallbackModelItem(item={}){const model=String(item?.ai_model||item?.model||"").trim(),apiUrl=String(item?.ai_api_url||item?.api_url||"").trim(),apiKey=String(item?.ai_api_key||item?.api_key||"").trim();return{model,api_url:apiUrl,api_key:apiKey}}
    function updateFallbackModelEmpty(){const list=$("aiFallbackModelList"),empty=$("aiFallbackModelEmpty");if(!list||!empty)return;empty.style.display=list.children.length?"none":"block"}
    function addFallbackModelRow(item={}){const normalized=normalizeFallbackModelItem(item),row=document.createElement("div");row.className="fallback-row";row.innerHTML=`<label class="field"><span>模型名</span><input type="text" data-fallback-model placeholder="例如：doubao-seed-character-251128" value="${esc(normalized.model)}"></label><label class="field"><span>API 地址</span><input type="text" data-fallback-api-url placeholder="留空则沿用主 API 地址" value="${esc(normalized.api_url)}"></label><label class="field"><span>API Key</span><input type="password" data-fallback-api-key placeholder="留空则沿用主 API Key" value="${esc(normalized.api_key)}"></label><button class="secondary" type="button" onclick="removeFallbackModelRow(this)">删除</button>`;$("aiFallbackModelList").appendChild(row);updateFallbackModelEmpty();return row}
    function removeFallbackModelRow(button){button.closest(".fallback-row")?.remove();updateFallbackModelEmpty()}
    function setFallbackModels(items){const list=$("aiFallbackModelList");if(!list)return;list.innerHTML="";for(const item of (items||[]).map(normalizeFallbackModelItem).filter(item=>item.model))addFallbackModelRow(item);updateFallbackModelEmpty()}
    function readFallbackModels(){return Array.from($("aiFallbackModelList")?.querySelectorAll(".fallback-row")||[]).map(row=>{const model=row.querySelector("[data-fallback-model]")?.value.trim()||"",apiUrl=row.querySelector("[data-fallback-api-url]")?.value.trim()||"",apiKey=row.querySelector("[data-fallback-api-key]")?.value.trim()||"";if(!model)return null;const item={model};if(apiUrl)item.api_url=apiUrl;if(apiKey)item.api_key=apiKey;return item}).filter(Boolean)}
    function setAiTestStatus(message="",type=""){const node=$("apiTestStatus");if(!node)return;node.textContent=message||"可点击“测试 API”验证当前主模型与备用模型链路。";node.style.color=type==="error"?"var(--bad)":type==="success"?"var(--good)":type==="info"?"var(--accent)":""}
    async function testAiApi(button=null){const apiKey=$("aiApiKey").value.trim(),apiModel=$("aiModel").value.trim()||DEFAULT_SETTINGS.ai_model,apiUrl=$("aiApiUrl").value.trim()||DEFAULT_SETTINGS.ai_api_url;if(!apiKey){const message="请先填写 API Key。";setAiTestStatus(message,"error");toast(message,"error");return}setAiTestStatus("正在测试当前主模型与备用模型链路，请稍等。","info");await withBusy(button,async()=>{const data=await api("/api/test-ai",{method:"POST",body:JSON.stringify({ai_api_key:apiKey,ai_model:apiModel,ai_api_url:apiUrl,ai_fallback_models:readFallbackModels()})});const label=data.active_label||data.active_model||apiModel,detail=data.used_fallback?`${label}（已切换到备用模型）`:label,message=`AI 连接成功：${detail}`;setAiTestStatus(message,"success");toast(message,"success")}).catch(error=>{const message=error.message||String(error);setAiTestStatus(message,"error");toast(message,"error")})}
    function renderClipOutputPreview(){const root=$("clipOutputRoot").value.trim();$("clipOutputPreview").textContent=root&&currentWorkspace?`${joinOutputPath(root,currentWorkspace)}`:"未设置时沿用工作间内部默认 clips 目录。"}
    function populate(task){
      const normalized=normalizeTask(task,currentWorkspace),firstBaidu=one(normalized.baidu_share),firstDouyin=one(normalized.douyin_download),firstAutoClip={...baseTask(currentWorkspace).auto_clip[0],...one(normalized.auto_clip)},settings=normalized.settings||{};
      currentTask=normalized;
      const shareUrl=firstBaidu.share_url||"";
      baiduListingUrl="";
      baiduExpandedFolders=new Set();
      baiduSavedEntries=normalized.baidu_share.map(item=>({name:item.target_filename||"",size:0,fs_id:item.target_fsid||"",path:item.target_path||""})).filter(item=>item.name||item.path||item.fs_id);
      selectedBaiduKeys=new Set(baiduSavedEntries.map(item=>baiduFileKey(item)).filter(Boolean));
      baiduFiles=[];
      setValue("baiduShareUrl",shareUrl);
      setValue("baiduOutputPath",firstBaidu.output_subdir||"downloads/baidu");
      setValue("baiduDownloadThreads",firstBaidu.download_threads||4);
      setValue("baiduDownloadMode",firstBaidu.download_mode||"api");
      setChecked("baiduSkipExisting",firstBaidu.skip_existing!==false);
      renderBaiduFiles();
      setDouyinLinks(normalized.douyin_download.map(item=>item.url||"").filter(Boolean));
      setValue("douyinOutputPath",firstDouyin.output_subdir||"downloads/douyin");
      setChecked("douyinWithWatermark",firstDouyin.with_watermark);
      setChecked("douyinOverwrite",firstDouyin.overwrite);
      updateDouyinCount();
      setValue("clipOutputRoot",settings.clip_output_root||inferClipOutputRoot(firstAutoClip.output_subdir,currentWorkspace));
      setChecked("forceNoNarrationMode",settings.force_no_narration_mode===true);
      setValue("narrationBackgroundPercent",settings.narration_background_percent??DEFAULT_SETTINGS.narration_background_percent);
      setValue("outputWatermarkText",settings.output_watermark_text||DEFAULT_SETTINGS.output_watermark_text);
      setChecked("enableAiNarrationRewrite",settings.disable_ai_narration_rewrite!==true);
      setValue("aiApiKey",settings.ai_api_key||"");
      setValue("aiModel",settings.ai_model||DEFAULT_SETTINGS.ai_model);
      setValue("aiApiUrl",settings.ai_api_url||DEFAULT_SETTINGS.ai_api_url);
      setFallbackModels(settings.ai_fallback_models||[]);
      setAiTestStatus();
      setChecked("autoEnableRandomEpisodeFlip",settings.enable_random_episode_flip!==false);
      setValue("autoRandomEpisodeFlipRatio",settings.random_episode_flip_ratio??.4);
      setChecked("autoEnableRandomVisualFilter",settings.enable_random_visual_filter!==false);
      renderClipOutputPreview();
      renderImportStatuses();
      setValue("concurrencyBaidu",normalized.concurrency.baidu_share||1);
      setValue("concurrencyDouyin",normalized.concurrency.douyin_download||3);
      setValue("concurrencyAutoClip",normalized.concurrency.auto_clip||1);
      renderMeta()
    }
    function buildTask(){
      if(!currentWorkspace)throw new Error("请先新建或选择一个工作间。");
      const task=normalizeTask(currentTask||{},currentWorkspace),shareUrl=$("baiduShareUrl").value.trim(),selectedKeys=Array.from(selectedBaiduKeys),baiduOutput=$("baiduOutputPath").value.trim()||"downloads/baidu",baiduThreads=asInt("baiduDownloadThreads",4),baiduMode=$("baiduDownloadMode").value||"api",douyinOutput=$("douyinOutputPath").value.trim()||"downloads/douyin",baiduLookup=new Map([...baiduSavedEntries,...baiduFiles].map(item=>[baiduFileKey(item),item]));
      if(shareUrl&&!selectedKeys.length)throw new Error("已填写网盘链接，请先提取内容并勾选要下载的视频。");
      task.baidu_share=shareUrl?selectedKeys.map(key=>{const matched=baiduLookup.get(key)||{};return{share_url:shareUrl,target_filename:String(matched.name||matched.server_filename||"").trim(),target_path:String(matched.path||"").trim(),target_fsid:String(matched.fs_id||"").trim(),output_subdir:baiduOutput,download_threads:baiduThreads,download_mode:baiduMode,skip_existing:$("baiduSkipExisting").checked}}):[];
      task.douyin_download=getDouyinLinks().map(url=>({url,output_subdir:douyinOutput,with_watermark:$("douyinWithWatermark").checked,overwrite:$("douyinOverwrite").checked}));
      task.subtitle_extract=[];
      const clipOutputRoot=$("clipOutputRoot").value.trim(),autoClipBase={...baseTask(currentWorkspace).auto_clip[0],...one(task.auto_clip)},flipRatioValue=Number.parseFloat($("autoRandomEpisodeFlipRatio").value),flipRatio=Number.isFinite(flipRatioValue)?Math.max(0,Math.min(1,flipRatioValue)):.4;
      task.settings={...(task.settings||{}),clip_output_root:clipOutputRoot,ai_api_key:$("aiApiKey").value.trim(),ai_model:$("aiModel").value.trim()||DEFAULT_SETTINGS.ai_model,ai_api_url:$("aiApiUrl").value.trim()||DEFAULT_SETTINGS.ai_api_url,ai_fallback_models:readFallbackModels(),prefer_funasr_audio_subtitles:true,disable_ai_subtitle_review:false,disable_ai_narration_rewrite:!$("enableAiNarrationRewrite").checked,prefer_funasr_sentence_pauses:true,force_no_narration_mode:$("forceNoNarrationMode").checked,narration_background_percent:Math.max(0,Math.min(100,asInt("narrationBackgroundPercent",DEFAULT_SETTINGS.narration_background_percent,0))),output_watermark_text:$("outputWatermarkText").value.trim(),enable_random_episode_flip:$("autoEnableRandomEpisodeFlip").checked,random_episode_flip_ratio:flipRatio,enable_random_visual_filter:$("autoEnableRandomVisualFilter").checked};
      task.auto_clip=[{...autoClipBase,reference_video_glob:autoClipBase.reference_video_glob||"downloads/douyin/*",reference_subtitle_glob:"",source_dir:autoClipBase.source_dir||"downloads/baidu",output_subdir:clipOutputRoot?joinOutputPath(clipOutputRoot,currentWorkspace):(autoClipBase.output_subdir||"clips"),title:autoClipBase.title||"{workspace_name}_{reference_stem}",match_all_references:autoClipBase.match_all_references!==false,skip_existing:autoClipBase.skip_existing!==false,keep_temp:!!autoClipBase.keep_temp}];
      task.concurrency={...task.concurrency,baidu_share:asInt("concurrencyBaidu",1),douyin_download:asInt("concurrencyDouyin",3),subtitle_extract:1,auto_clip:asInt("concurrencyAutoClip",1)};
      return task
    }
    async function refreshBaiduLoginState(button=null,silent=true){await withBusy(button,async()=>{baiduLoginState=await api("/api/baidu/login-status");renderBaiduLoginState();if(!silent){if(baiduLoginState.profile_in_use)toast("百度专用登录窗口还开着，关闭后再提取或开始处理。","info");else if(baiduLoginState.logged_in)toast("百度专用窗口已登录，可以继续。","success");else toast("百度专用窗口还没登录。","error")}}).catch(error=>{if(!silent)toast(error.message,"error")})}
    async function openBaiduLogin(button){await withBusy(button,async()=>{const data=await api("/api/baidu/open-login",{method:"POST",body:JSON.stringify({})});baiduLoginState=data.login_state||null;renderBaiduLoginState();if(data.already_open)toast("百度专用登录窗口已经打开，先在里面完成登录并关闭。","info");else if(baiduLoginState&&baiduLoginState.logged_in)toast("检测到百度专用窗口已经有登录态，可直接开始处理。","success");else toast("已打开百度专用登录窗口，请在里面完成登录后关闭，再开始处理。","success")}).catch(error=>toast(error.message,"error"))}
    async function extractBaiduFiles(button){const shareUrl=$("baiduShareUrl").value.trim();if(!shareUrl){toast("请先填写百度网盘链接。","error");return}baiduListingPending=true;baiduListingUrl=shareUrl;baiduFiles=[];selectedBaiduKeys=new Set();baiduExpandedFolders.clear();renderBaiduFiles();toast("正在提取网盘内容，请稍等，不要重复点击。","info");await withBusy(button,async()=>{const data=await api("/api/baidu/list-share",{method:"POST",body:JSON.stringify({share_url:shareUrl})});baiduFiles=(data.files||[]).map(item=>({name:String(item.name||item.server_filename||"").trim(),size:item.size||0,fs_id:item.fs_id||"",path:String(item.path||"").trim()})).filter(item=>item.name);baiduExpandedFolders=collectDefaultOpenBaiduFolders(buildBaiduTree(baiduFiles));baiduSavedEntries=[...baiduFiles];selectedBaiduKeys=new Set(baiduFiles.map(item=>baiduFileKey(item)));await refreshBaiduLoginState(null,true);renderBaiduFiles();toast(`已提取 ${baiduFiles.length} 个视频，默认已全选。`,"success")}).catch(error=>{baiduFiles=[];toast(error.message,"error")}).finally(()=>{baiduListingPending=false;renderBaiduFiles()})}
    function statusClass(status){if(status==="completed")return"good";if(status==="failed")return"bad";if(status==="stopping")return"warn";if(status==="idle"||!status)return"idle";return"run"}
    function formatTime(value){return value?new Date(value*1000).toLocaleString("zh-CN"):"-"}
    function shortPathName(value){const parts=String(value||"").split(/[\\/]/).filter(Boolean);return parts.length?parts[parts.length-1]:String(value||"")}
    function runtimeNeedsBaiduDiagnosis(runtime){return Boolean(runtime?.progress?.blocked&&runtime?.progress?.stageKey==="baidu_share")}
    function clearBaiduDiagnosisState(workspaceName){if(workspaceName)baiduOfficialDiagnosisCache.delete(workspaceName)}
    function getBaiduDiagnosisState(workspaceName){return workspaceName?baiduOfficialDiagnosisCache.get(workspaceName)||null:null}
    function renderBaiduDiagnosisMarkup(workspace,runtime){
      const state=getBaiduDiagnosisState(workspace?.name||""),hasBaiduTask=Number(workspace?.task_summary?.baidu_share||0)>0;
      if(!workspace||(!hasBaiduTask&&!state?.loading&&!state?.error&&!state?.data))return"";
      const actions=[`<button class="secondary" type="button" onclick="diagnoseBlockedRuntime(this)">${state?.data?"重新诊断":"诊断下载卡点"}</button>`];
      const logPath=runtime.job?.log_path||state?.data?.log_path||"";
      if(logPath)actions.push(`<button class="secondary" type="button" onclick="openPathAction('${enc(logPath)}',this,'${enc("日志目录")}')">打开日志目录</button>`);
      if(workspace.path)actions.push(`<button class="secondary" type="button" onclick="openPathAction('${enc(workspace.path)}',this,'${enc("工作间目录")}')">打开工作间目录</button>`);
      const blockedLike=runtimeNeedsBaiduDiagnosis(runtime)||Boolean(state?.data?.blocked);
      let body=`<div class="runtime-diagnosis loading"><div class="runtime-diag-summary">${blockedLike?"这一步已经判定为“明显卡住”。点击“重新诊断”后，我会检查当前监控目录、百度客户端进程和最近下载记录。":"如果你怀疑百度网盘官方客户端下载卡住了，可以点“诊断下载卡点”直接检查上一次日志里的监控目录、客户端下载进程和同名文件情况。"}</div></div>`;
      if(state?.loading){
        body=`<div class="runtime-diagnosis loading"><div class="runtime-diag-summary">正在诊断当前卡点，请稍等...</div></div>`;
      }else if(state?.error){
        body=`<div class="runtime-diagnosis error"><div class="runtime-diag-summary">${esc(state.error)}</div></div>`;
      }else if(state?.data){
        const data=state.data,stats=[{value:`${data.ready_count||0}/${data.total_count||0}`,label:"已到位原素材"},{value:String(data.pending_count||0),label:"待处理文件"},{value:String(data.search_root_count||0),label:"监控目录"},{value:String(data.process_count||0),label:"百度进程"}];
        const suggestionItems=(data.suggestions||[]).slice(0,4).map(item=>`<div class="runtime-diag-item"><strong>建议</strong><small>${esc(item)}</small></div>`).join("");
        const pendingItems=(data.pending_details||[]).slice(0,4).map(item=>{const candidate=(item.root_matches||[])[0],record=(item.client_records||[])[0],detail=candidate?`${candidate.time_hint}${candidate.size_hint?`，${candidate.size_hint}`:""}${candidate.modified_at?`，时间 ${candidate.modified_at}`:""}`:(record?`客户端下载记录路径：${record.local_path}${record.added_at?`，记录时间 ${record.added_at}`:""}`:"当前没在监控目录或客户端下载记录里发现它");return`<div class="runtime-diag-item"><strong>${esc(item.label)}：${esc(item.state_label||"待检查")}</strong><small>${esc(detail)}</small></div>`}).join("");
        const pathButtons=(data.action_paths||[]).slice(0,6).map(item=>`<button class="secondary" type="button" title="${esc(item.path||"")}" onclick="openPathAction('${enc(item.path||"")}',this,'${enc(item.label||"路径")}')">打开${esc(item.label||shortPathName(item.path||"路径"))}</button>`).join("");
        body=`<div class="runtime-diagnosis"><div class="runtime-diag-grid">${stats.map(item=>`<div class="runtime-diag-stat"><strong>${esc(item.value)}</strong><span>${esc(item.label)}</span></div>`).join("")}</div><div class="runtime-diag-summary">${esc(data.summary||"")}${data.process_summary?`<br>${esc(data.process_summary)}`:""}${data.wait_text?`<br>已持续等待：${esc(data.wait_text)}`:""}</div>${suggestionItems?`<div class="runtime-diag-list">${suggestionItems}</div>`:""}${pendingItems?`<div class="runtime-diag-list">${pendingItems}</div>`:""}${pathButtons?`<div class="runtime-diag-actions">${pathButtons}</div>`:""}</div>`;
      }
      return`<div class="runtime-tools">${actions.join("")}</div>${body}`;
    }
    function maybeAutoDiagnoseBlockedRuntime(workspace,runtime){
      if(!workspace||!runtimeNeedsBaiduDiagnosis(runtime))return;
      const cached=getBaiduDiagnosisState(workspace.name);
      if(cached?.loading)return;
      if(cached?.data&&Date.now()-Number(cached.fetchedAt||0)<60000)return;
      if(cached?.error&&Date.now()-Number(cached.fetchedAt||0)<20000)return;
      diagnoseBlockedRuntime(null,true)
    }
    async function diagnoseBlockedRuntime(button=null,silent=false){
      const workspaceName=currentWorkspace;
      if(!workspaceName){if(!silent)toast("请先选择工作间。","error");return}
      const state=getBaiduDiagnosisState(workspaceName);
      if(state?.loading)return;
      baiduOfficialDiagnosisCache.set(workspaceName,{...(state||{}),loading:true,error:"",fetchedAt:Date.now()});
      if(currentWorkspace===workspaceName)renderMeta();
      await withBusy(button,async()=>{
        const data=await api(`/api/workspaces/${encodeURIComponent(workspaceName)}/baidu-official-diagnosis`);
        baiduOfficialDiagnosisCache.set(workspaceName,{loading:false,error:"",fetchedAt:Date.now(),data});
        if(currentWorkspace===workspaceName)renderMeta();
        if(!silent)toast("已更新卡点诊断。","success")
      }).catch(error=>{
        baiduOfficialDiagnosisCache.set(workspaceName,{loading:false,error:error.message||String(error),fetchedAt:Date.now(),data:null});
        if(currentWorkspace===workspaceName)renderMeta();
        if(!silent)toast(error.message,"error")
      })
    }
    async function openPathAction(encodedPath,button=null,encodedLabel=""){
      const path=decodeURIComponent(encodedPath||""),label=decodeURIComponent(encodedLabel||"路径");
      await withBusy(button,async()=>{await api("/api/open-path",{method:"POST",body:JSON.stringify({path})});toast(`已打开${label}。`,"success")}).catch(error=>toast(error.message,"error"))
    }
    function renderWorkspaces(){
      const workspaces=latestStatus?.workspaces||[];
      $("workspaceCount").textContent=`${workspaces.length} 个`;
      if(!workspaces.length){
        $("workspaceList").innerHTML=`<div class="empty">还没有工作间。</div>`;
        applySidebarState();
        return;
      }
      $("workspaceList").innerHTML=workspaces.map(workspace=>{
        const summary=workspace.task_summary||{},assets=workspace.asset_summary||{},sourceCount=assetSectionCount(assets.source),referenceCount=assetSectionCount(assets.reference),subtitleCount=assetSectionCount(assets.subtitle);
        const runtime=runtimeForWorkspace(workspace.name);
        const name=esc(workspace.name);
        const encoded=enc(workspace.name);
        const savedState=workspace.has_task?"good":"warn";
        const progressMarkup=renderProgressMarkup(runtime.progress,true);
        return`<article class="workspace-card ${workspace.name===currentWorkspace?"active":""}" onclick="selectWorkspace('${encoded}',true)"><div class="row"><label class="check" onclick="event.stopPropagation()"><input type="checkbox" data-name="${name}" ${selectedWorkspaces.has(workspace.name)?"checked":""} onchange="toggleWorkspace(this)"><span>勾选</span></label><span class="pill ${runtime.status!=="idle"?runtime.className:savedState}">${runtime.status!=="idle"?runtime.badge:(workspace.has_task?"已保存":"未保存")}</span></div><h3>${name}</h3><div class="workspace-status"><div class="status-line"><div class="status-main ${runtime.className}"><span class="status-dot ${runtime.status==='running'?'is-running':''}"></span>${esc(runtime.label)}</div>${runtime.job?`<span class="pill ${runtime.className}">${esc(runtime.badge)}</span>`:""}</div><div class="status-detail">${esc(runtime.detail)}<br>${esc(runtime.preview)}</div>${progressMarkup}</div><div class="chips"><span class="pill">原素材文件 ${sourceCount}</span><span class="pill">参考视频 ${referenceCount}</span><span class="pill">字幕文件 ${subtitleCount}</span><span class="pill">剪辑任务 ${summary.auto_clip||0}</span></div><div class="actions" style="margin-top:10px"><button class="secondary" type="button" onclick="event.stopPropagation();selectWorkspace('${encoded}',true)">进入编辑</button><button type="button" onclick="event.stopPropagation();runWorkspace('${encoded}',this)">启动</button><button class="danger-soft" type="button" onclick="event.stopPropagation();deleteWorkspace('${encoded}',this)">删除</button></div></article>`
      }).join("");
      applySidebarState();
    }
    function renderJobs(){
      const jobs=latestStatus?.jobs||[];
      if(activeJobId&&!jobs.some(job=>job.job_id===activeJobId))activeJobId="";
      if(!activeJobId&&jobs.length)activeJobId=jobs[0].job_id;
      if(!jobs.length){
        $("jobList").innerHTML=`<div class="empty">暂无运行任务。</div>`;
        updateLog();
        applySidebarState();
        return;
      }
      $("jobList").innerHTML=jobs.map(job=>{
        const encoded=enc(job.job_id);
        const members=jobWorkspaceMembers(job);
        const displayWorkspace=members.includes(currentWorkspace)?currentWorkspace:(members[0]||currentWorkspace);
        const progress=analyzeJobProgress(job,displayWorkspace);
        const preview=progress.note||latestJobPreview(job);
        const title=members.length>1?`批量运行：${members.length} 个工作间`:repairText(job.workspace||job.job_id,members[0]||currentWorkspace);
        return`<article class="job-card ${job.job_id===activeJobId?"active":""}"><div class="row"><h3>${esc(title)}</h3><span class="pill ${statusClass(job.status)}">${esc(progress.totalStages?`${progress.percent}%`:jobStatusLabel(job.status))}</span></div><div class="muted">${esc(formatJobMeta(job))}</div>${renderProgressMarkup(progress,true)}<div class="job-preview">${esc(preview)}</div><div class="actions" style="margin-top:10px"><button class="secondary" type="button" onclick="setActiveJob('${encoded}')">看日志</button><button class="danger" type="button" onclick="stopJob('${encoded}',this)" ${job.status!=="running"?"disabled":""}>停止</button></div></article>`
      }).join("");
      updateLog();
      applySidebarState();
    }
    function renderMeta(){
      const workspace=(latestStatus?.workspaces||[]).find(item=>item.name===currentWorkspace);
      const summary=workspace?.task_summary||{};
      const assets=workspace?.asset_summary||{};
      const runtime=runtimeForWorkspace(currentWorkspace);
      const banner=$("workspaceRuntimeBanner");
      $("currentWorkspaceName").textContent=currentWorkspace?`当前工作间：${currentWorkspace}`:"当前工作间";
      $("workspaceMeta").textContent=currentWorkspace?`${repairText(workspace?.task_path||"task.json",currentWorkspace)} | ${repairText(workspace?.path||"",currentWorkspace)}`:"先新建或选择一个工作间。";
      $("workspaceBadges").innerHTML=currentWorkspace?`<span class="pill">原素材文件 ${assetSectionCount(assets.source)}</span><span class="pill">参考视频 ${assetSectionCount(assets.reference)}</span><span class="pill">字幕文件 ${assetSectionCount(assets.subtitle)}</span><span class="pill">剪辑任务 ${summary.auto_clip||0}</span>`:"";
      renderImportStatuses();
      if(!banner)return;
      if(!currentWorkspace){
        banner.className="runtime-banner idle";
        banner.innerHTML=`<div class="runtime-top"><div class="runtime-title idle"><span class="status-dot"></span>当前状态：空闲</div><span class="pill idle">未启动</span></div><div class="runtime-detail">启动工作间后，这里会直接显示它是否正在运行、现在做到第几步，以及当前进度百分比。</div><div class="runtime-meta"><span class="pill idle">等待任务</span></div>`;
        return;
      }
      banner.className=`runtime-banner ${runtime.className}`;
      banner.innerHTML=`<div class="runtime-top"><div class="runtime-title ${runtime.className}"><span class="status-dot ${runtime.status==='running'?'is-running':''}"></span>当前状态：${esc(runtime.label)}</div><span class="pill ${runtime.className}">${esc(runtime.badge)}</span></div><div class="runtime-detail">${esc(runtime.detail)}<br>${esc(runtime.preview)}</div>${renderProgressMarkup(runtime.progress,false)}<div class="runtime-meta"><span class="pill ${runtime.className}">${runtime.job?`最近状态：${jobStatusLabel(runtime.job.status)}`:"暂无运行记录"}</span>${runtime.job?.log_path?`<span class="pill">${esc(repairText(runtime.job.log_path,currentWorkspace))}</span>`:""}</div>${renderBaiduDiagnosisMarkup(workspace,runtime)}`;
      maybeAutoDiagnoseBlockedRuntime(workspace,runtime);
    }
    async function loadStatus(silent=true,button=null){await withBusy(button,async()=>{latestStatus=await api("/api/status");const server=latestStatus.server||{},runningCount=(latestStatus.jobs||[]).filter(job=>["running","stopping"].includes(job.status)).length;$("serverInfo").textContent=`${server.host}:${server.port} | 进程 ${server.pid||"-"} | 运行中 ${runningCount}`;if(currentWorkspace&&!latestStatus.workspaces.some(item=>item.name===currentWorkspace)){clearBaiduDiagnosisState(currentWorkspace);currentWorkspace="";currentTask=null;selectedWorkspaces.clear();populate(baseTask(""))}if(!currentWorkspace&&latestStatus.workspaces.length){currentWorkspace=latestStatus.workspaces[0].name;selectedWorkspaces.add(currentWorkspace);await loadTask()}renderWorkspaces();renderJobs();renderMeta();applySidebarState()}).catch(error=>{if(!silent)toast(error.message,"error")})}
    async function loadTask(){if(!currentWorkspace){populate(baseTask(""));return}const data=await api(`/api/workspaces/${encodeURIComponent(currentWorkspace)}/task`);populate(normalizeTask(data.task,currentWorkspace));renderMeta()}
    async function reloadCurrentTask(button=null){if(!currentWorkspace){toast("请先选择工作间。","error");return}await withBusy(button,async()=>{await loadTask();toast("已重新载入当前配置。","success")}).catch(error=>toast(error.message,"error"))}
function selectWorkspace(name,revealEditor=false){const nextWorkspace=decodeURIComponent(name),unchanged=nextWorkspace===currentWorkspace;currentWorkspace=nextWorkspace;selectedWorkspaces.add(currentWorkspace);if(revealEditor)focusWorkspaceEditor();if(unchanged){renderMeta();return}loadTask().then(()=>{loadStatus(true);if(revealEditor)focusWorkspaceEditor()}).catch(error=>toast(error.message,"error"))}
    function toggleWorkspace(input){const name=input.dataset.name;if(!name)return;input.checked?selectedWorkspaces.add(name):selectedWorkspaces.delete(name);renderWorkspaces()}
    async function createWorkspace(button){await withBusy(button,async()=>{const name=$("newWorkspaceName").value.trim();if(!name)throw new Error("请先填写工作间名称。");const task=baseTask(name);await api(`/api/workspaces/${encodeURIComponent(name)}/task`,{method:"POST",body:JSON.stringify({task})});$("newWorkspaceName").value="";clearBaiduDiagnosisState(name);currentWorkspace=name;currentTask=task;selectedWorkspaces.add(name);populate(task);await loadStatus(true);toast(`已新建工作间：${name}`,"success")}).catch(error=>toast(error.message,"error"))}
    async function renameCurrentWorkspace(button){await withBusy(button,async()=>{if(!currentWorkspace)throw new Error("请先选择要改名的工作间。");const oldName=currentWorkspace,nextRaw=window.prompt("请输入新的工作间名称。",oldName);if(nextRaw===null)return;const newName=String(nextRaw||"").trim();if(!newName)throw new Error("新的工作间名称不能为空。");if(newName===oldName){toast("工作间名称没有变化。","info");return}await saveCurrentWorkspace(false);const data=await api(`/api/workspaces/${encodeURIComponent(oldName)}/rename`,{method:"POST",body:JSON.stringify({new_name:newName})});clearBaiduDiagnosisState(oldName);selectedWorkspaces.delete(oldName);currentWorkspace=data.workspace||newName;selectedWorkspaces.add(currentWorkspace);currentTask=null;await loadStatus(true);await loadTask();toast(`已将工作间改名为：${currentWorkspace}`,"success")}).catch(error=>toast(error.message,"error"))}
    async function deleteWorkspace(encodedName="",button=null){const workspace=encodedName?decodeURIComponent(encodedName):currentWorkspace;if(!workspace){toast("请先选择要删除的工作间。","error");return}if(!window.confirm(`确定删除工作间「${workspace}」吗？该工作间里的下载、字幕、日志和相关缓存都会一起删除。`))return;await withBusy(button,async()=>{const data=await api(`/api/workspaces/${encodeURIComponent(workspace)}/delete`,{method:"POST",body:JSON.stringify({})});clearBaiduDiagnosisState(workspace);selectedWorkspaces.delete(workspace);if(currentWorkspace===workspace){currentWorkspace="";currentTask=null;populate(baseTask(""))}await loadStatus(true);const cacheCount=Number(data.cleared_job_cache_count||0)+Number(data.cleared_external_log_count||0);toast(`已删除工作间：${workspace}${cacheCount?`，并清理 ${cacheCount} 项相关缓存`:""}`,"success")}).catch(error=>toast(error.message,"error"))}
    async function saveCurrentWorkspace(showToast=true,button=null){return await withBusy(button,async()=>{const task=buildTask();await api(`/api/workspaces/${encodeURIComponent(currentWorkspace)}/task`,{method:"POST",body:JSON.stringify({task})});currentTask=task;await loadStatus(true);if(showToast)toast(`已保存：${currentWorkspace}`,"success");return task}).catch(error=>{toast(error.message,"error");throw error})}
    async function saveAndRunCurrentWorkspace(button){await withBusy(button,async()=>{await saveCurrentWorkspace(false);clearBaiduDiagnosisState(currentWorkspace);const data=await api("/api/run-workspace",{method:"POST",body:JSON.stringify({workspace:currentWorkspace})});activeJobId=data?.job?.job_id||activeJobId;collapseSidebarAfterRun();await loadStatus(true);toast(`已启动：${currentWorkspace}`,"success")}).catch(error=>toast(error.message,"error"))}
    async function runWorkspace(name,button=null){await withBusy(button,async()=>{const workspace=decodeURIComponent(name);clearBaiduDiagnosisState(workspace);const data=await api("/api/run-workspace",{method:"POST",body:JSON.stringify({workspace})});activeJobId=data?.job?.job_id||activeJobId;collapseSidebarAfterRun();await loadStatus(true);toast(`已启动：${workspace}`,"success")}).catch(error=>toast(error.message,"error"))}
    async function runSelectedWorkspaces(button){await withBusy(button,async()=>{const workspaces=Array.from(selectedWorkspaces);if(!workspaces.length&&currentWorkspace)workspaces.push(currentWorkspace);if(!workspaces.length)throw new Error("请先勾选至少一个工作间。");if(currentWorkspace&&workspaces.includes(currentWorkspace))await saveCurrentWorkspace(false);for(const workspace of workspaces)clearBaiduDiagnosisState(workspace);const data=await api("/api/run-batch",{method:"POST",body:JSON.stringify({workspaces,workspace_parallel:asInt("workspaceParallel",2)})});activeJobId=data?.job?.job_id||activeJobId;collapseSidebarAfterRun();await loadStatus(true);toast(`已启动 ${workspaces.length} 个工作间。`,"success")}).catch(error=>toast(error.message,"error"))}
    async function importLocalPaths(kind,textareaId,button){await withBusy(button,async()=>{if(!currentWorkspace)throw new Error("请先选择工作间。");const paths=lines($(textareaId).value);if(!paths.length)throw new Error("请先填写本地文件或文件夹路径。");const data=await api(`/api/workspaces/${encodeURIComponent(currentWorkspace)}/import-local`,{method:"POST",body:JSON.stringify({kind,paths})});$(textareaId).value="";await loadStatus(true);toast(`已导入 ${data.count||0} 个文件。`,"success")}).catch(error=>toast(error.message,"error"))}
    async function stopJob(id,button=null){await withBusy(button,async()=>{const job_id=decodeURIComponent(id);await api("/api/stop-job",{method:"POST",body:JSON.stringify({job_id})});await loadStatus(true);toast("停止请求已发出。","info")}).catch(error=>toast(error.message,"error"))}
    function setActiveJob(id){activeJobId=decodeURIComponent(id);updateLog()}
    function stopActiveJob(button){if(activeJobId)stopJob(enc(activeJobId),button)}
    function updateLog(){const job=(latestStatus?.jobs||[]).find(item=>item.job_id===activeJobId);if(!job){$("logTitle").textContent="暂无运行日志。";$("logOutput").textContent="暂无运行日志。";$("stopActiveJobButton").disabled=true;return}const members=jobWorkspaceMembers(job),displayWorkspace=members.includes(currentWorkspace)?currentWorkspace:(members[0]||currentWorkspace),label=members.length>1?`批量运行：${members.join("、")}`:repairText(job.workspace||job.job_id,members[0]||currentWorkspace),lines=localizedJobLines(job),progress=analyzeJobProgress(job,displayWorkspace),progressLabel=progress.totalStages?` | ${progress.stepText} | ${progress.percent}%`:"",logLabel=job.log_path?` | 日志文件：${repairText(job.log_path||"",displayWorkspace)}`:"";$("logTitle").textContent=`${label} | ${jobStatusLabel(job.status)}${progressLabel}${logLabel}`;$("logOutput").textContent=lines.join("\n")||"任务已启动，正在等待输出...";$("stopActiveJobButton").disabled=job.status!=="running"}
    async function shutdownControlCenter(button){const running=(latestStatus?.jobs||[]).filter(job=>["running","stopping"].includes(job.status));if(running.length&&!window.confirm("当前还有运行任务。关闭控制台服务不会自动关闭已启动任务，确定继续吗？"))return;await withBusy(button,async()=>{await api("/api/shutdown",{method:"POST",body:JSON.stringify({})});toast("控制台服务正在关闭。","success");$("serverInfo").textContent="服务已请求关闭";setTimeout(()=>{try{window.close()}catch(error){}},800)}).catch(error=>toast(error.message,"error"))}
    $("clipOutputRoot").addEventListener("input",renderClipOutputPreview);
    $("baiduShareUrl").addEventListener("input",()=>{const url=$("baiduShareUrl").value.trim();if(baiduListingUrl&&url!==baiduListingUrl){baiduListingUrl="";baiduFiles=[];baiduSavedEntries=[];selectedBaiduKeys.clear();baiduExpandedFolders.clear();renderBaiduFiles()}});
    function refreshDashboard(){loadStatus(true);refreshBaiduLoginState(null,true)}
    window.addEventListener("focus",()=>refreshBaiduLoginState(null,true));
    document.addEventListener("visibilitychange",()=>{if(!document.hidden)refreshBaiduLoginState(null,true)});
    window.addEventListener("resize",()=>applySidebarState());
    loadSidebarState();
    populate(baseTask(""));
    applySidebarState();
    loadStatus(false);
    refreshBaiduLoginState(null,true);
    setInterval(refreshDashboard,5000);
  