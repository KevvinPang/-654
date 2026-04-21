const fs = require('fs');
const path = 'E:/服务器自动剪辑/control_center_ui.html';
const html = fs.readFileSync(path, 'utf8');
const match = html.match(/<script>([\s\S]*)<\/script>/i);
if (!match) throw new Error('script block not found');
const out = 'E:/服务器自动剪辑/.tmp_control_center_ui_check.js';
fs.writeFileSync(out, match[1], 'utf8');
