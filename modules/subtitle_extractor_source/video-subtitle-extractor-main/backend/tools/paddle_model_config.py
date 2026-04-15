import os
import shutil
import tempfile
import time
from pathlib import Path

from backend.config import BASE_DIR, config


class PaddleModelConfig:
    def __init__(self, hardware_accelerator):
        self.hardware_accelerator = hardware_accelerator
        # 设置识别语言
        self.REC_CHAR_TYPE = config.language.value

        # 模型文件目录
        self.MODEL_BASE = os.path.join(BASE_DIR, 'models')
        self.ASCII_MODEL_CACHE_ROOT = Path(
            os.environ.get(
                "SERVER_AUTO_CLIP_ASCII_MODEL_CACHE",
                os.path.join(tempfile.gettempdir(), "server_auto_clip_ascii_models"),
            )
        )
        # 模型版本 V5
        self.MODEL_VERSION = 'V5'
        # V5模型默认图形识别的shape为3, 48, 320
        self.REC_IMAGE_SHAPE = '3,48,320'
        # 初始化模型路径
        self.REC_MODEL_PATH = None
        self.DET_MODEL_PATH = None
        self.DET_MODEL_NAME = None
        self.REC_MODEL_NAME = None

        # 语言组定义
        self.LATIN_LANG = [
            'af', 'az', 'bs', 'cs', 'cy', 'da', 'de', 'es', 'et', 'fr', 'ga', 'hr',
            'hu', 'id', 'is', 'it', 'ku', 'la', 'lt', 'lv', 'mi', 'ms', 'mt', 'nl',
            'no', 'oc', 'pi', 'pl', 'pt', 'ro', 'rs_latin', 'sk', 'sl', 'sq', 'sv',
            'sw', 'tl', 'tr', 'uz', 'vi', 'latin', 'german', 'french',
            'fi', 'eu', 'gl', 'lb', 'rm', 'ca', 'qu',
        ]
        self.ARABIC_LANG = ['ar', 'fa', 'ug', 'ur', 'ps', 'sd', 'bal']
        self.CYRILLIC_LANG = [
            'ru', 'rs_cyrillic', 'be', 'bg', 'uk', 'mn', 'abq', 'ady', 'kbd', 'ava',
            'dar', 'inh', 'che', 'lbe', 'lez', 'tab', 'cyrillic',
            'sr', 'kk', 'ky', 'tg', 'mk', 'tt', 'cv', 'ba', 'mhr', 'mo',
            'udm', 'kv', 'os', 'bua', 'xal', 'tyv', 'sah', 'kaa',
        ]
        self.DEVANAGARI_LANG = [
            'hi', 'mr', 'ne', 'bh', 'mai', 'ang', 'bho', 'mah', 'sck', 'new', 'gom',
            'sa', 'bgc', 'devanagari',
        ]
        self.OTHER_LANG = [
            'ch', 'japan', 'korean', 'en', 'ta', 'kn', 'te', 'ka',
            'chinese_cht',
        ]
        self.MULTI_LANG = (self.LATIN_LANG + self.ARABIC_LANG + self.CYRILLIC_LANG
                           + self.DEVANAGARI_LANG + self.OTHER_LANG)

        # 如果设置了识别文本语言类型，则设置为对应的语言
        if self.REC_CHAR_TYPE in self.MULTI_LANG:
            resolved = self._resolve_models()
            if resolved:
                self.MODEL_VERSION = 'V5'
                self.DET_MODEL_PATH, self.REC_MODEL_PATH, self.DET_MODEL_NAME, self.REC_MODEL_NAME = resolved

    @staticmethod
    def _contains_non_ascii(text):
        try:
            str(text).encode("ascii")
            return False
        except UnicodeEncodeError:
            return True

    def _tree_needs_sync(self, source_dir, target_dir):
        source_dir = Path(source_dir)
        target_dir = Path(target_dir)
        if not target_dir.exists():
            return True
        for source_path in source_dir.rglob("*"):
            relative_path = source_path.relative_to(source_dir)
            target_path = target_dir / relative_path
            if source_path.is_dir():
                if not target_path.exists():
                    return True
                continue
            if not target_path.is_file():
                return True
            try:
                source_stat = source_path.stat()
                target_stat = target_path.stat()
            except OSError:
                return True
            if target_stat.st_size != source_stat.st_size:
                return True
            if target_stat.st_mtime + 1 < source_stat.st_mtime:
                return True
        return False

    def _sync_tree(self, source_dir, target_dir):
        source_dir = Path(source_dir)
        target_dir = Path(target_dir)
        target_dir.mkdir(parents=True, exist_ok=True)
        for source_path in source_dir.rglob("*"):
            relative_path = source_path.relative_to(source_dir)
            target_path = target_dir / relative_path
            if source_path.is_dir():
                target_path.mkdir(parents=True, exist_ok=True)
                continue
            target_path.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(source_path, target_path)

    def _acquire_lock(self, lock_path):
        deadline = time.time() + 300
        while True:
            try:
                return os.open(str(lock_path), os.O_CREAT | os.O_EXCL | os.O_RDWR)
            except FileExistsError:
                if time.time() > deadline:
                    try:
                        if lock_path.exists():
                            lock_age = time.time() - lock_path.stat().st_mtime
                            if lock_age > 300:
                                lock_path.unlink()
                                continue
                    except OSError:
                        pass
                    raise RuntimeError(f"Timed out while waiting for model cache lock: {lock_path}")
                time.sleep(0.2)

    def _ensure_ascii_model_dir(self, model_dir):
        source_dir = Path(model_dir)
        if not self._contains_non_ascii(source_dir):
            return str(source_dir)

        cache_dir = self.ASCII_MODEL_CACHE_ROOT / source_dir.parent.name / source_dir.name
        lock_path = cache_dir.parent / f"{source_dir.name}.lock"
        lock_path.parent.mkdir(parents=True, exist_ok=True)
        lock_handle = self._acquire_lock(lock_path)
        try:
            if self._tree_needs_sync(source_dir, cache_dir):
                self._sync_tree(source_dir, cache_dir)
        finally:
            os.close(lock_handle)
            try:
                lock_path.unlink()
            except OSError:
                pass
        return str(cache_dir)

    def _get_v5_rec_model_name(self, lang):
        """
        根据语言获取V5识别模型目录名
        参考: https://www.paddleocr.ai/main/version3.x/algorithm/PP-OCRv5/PP-OCRv5_multi_languages.html
        """
        if lang in ('ch', 'chinese_cht', 'japan'):
            return 'PP-OCRv5_server_rec_infer'
        elif lang == 'en':
            return 'PP-OCRv5_server_rec_infer'
        elif lang == 'korean':
            return 'korean_PP-OCRv5_mobile_rec_infer'
        elif lang in self.LATIN_LANG:
            return 'latin_PP-OCRv5_mobile_rec_infer'
        elif lang in self.ARABIC_LANG:
            return 'arabic_PP-OCRv5_mobile_rec_infer'
        elif lang in self.CYRILLIC_LANG:
            return 'cyrillic_PP-OCRv5_mobile_rec_infer'
        elif lang in self.DEVANAGARI_LANG:
            return 'devanagari_PP-OCRv5_mobile_rec_infer'
        elif lang == 'th':
            return 'th_PP-OCRv5_mobile_rec_infer'
        elif lang == 'el':
            return 'el_PP-OCRv5_mobile_rec_infer'
        elif lang == 'ta':
            return 'ta_PP-OCRv5_mobile_rec_infer'
        elif lang == 'te':
            return 'te_PP-OCRv5_mobile_rec_infer'
        return None

    @staticmethod
    def _read_model_name_from_yaml(model_dir):
        """从 inference.yml 中读取 Global.model_name"""
        yaml_path = os.path.join(model_dir, 'inference.yml')
        if not os.path.exists(yaml_path):
            return None
        try:
            with open(yaml_path, 'r', encoding='utf-8') as f:
                in_global = False
                for line in f:
                    stripped = line.strip()
                    if stripped == 'Global:':
                        in_global = True
                        continue
                    if in_global:
                        if stripped and not stripped.startswith('#') and ':' in stripped:
                            if stripped.startswith('model_name:'):
                                return stripped.split(':', 1)[1].strip().strip('"').strip("'")
                        # 遇到下一个顶级 section 则退出
                        if stripped and not stripped.startswith('model_name') and not stripped.startswith(' ') and stripped.endswith(':'):
                            break
        except Exception:
            pass
        return None

    def _resolve_models(self):
        """
        解析 V5 模型路径，返回 (det_model_path, rec_model_path, det_model_name, rec_model_name) 或 None
        """
        v5_base = os.path.join(self.MODEL_BASE, 'V5')

        # 快速模式优先使用 mobile 模型，否则使用 server 模型
        if config.mode.value == 'fast':
            det_model_path = os.path.join(v5_base, 'PP-OCRv5_mobile_det_infer')
            if not os.path.exists(det_model_path):
                det_model_path = os.path.join(v5_base, 'PP-OCRv5_server_det_infer')
        else:
            det_model_path = os.path.join(v5_base, 'PP-OCRv5_server_det_infer')
        if not os.path.exists(det_model_path):
            return None

        det_model_path = self._ensure_ascii_model_dir(det_model_path)
        det_model_name = self._read_model_name_from_yaml(det_model_path)

        # 快速模式：中文(简/繁)、英文、日文使用通用 mobile 模型，其他语言使用对应的专用模型
        if config.mode.value == 'fast' and self.REC_CHAR_TYPE in ('ch', 'chinese_cht', 'en', 'japan'):
            rec_model_path = os.path.join(v5_base, 'PP-OCRv5_mobile_rec_infer')
            if os.path.exists(rec_model_path):
                rec_model_path = self._ensure_ascii_model_dir(rec_model_path)
                rec_model_name = self._read_model_name_from_yaml(rec_model_path)
                return det_model_path, rec_model_path, det_model_name, rec_model_name
            # mobile 不存在则 fallback 到按语言选择

        # 获取识别模型
        rec_model_dir_name = self._get_v5_rec_model_name(self.REC_CHAR_TYPE)
        if rec_model_dir_name is None:
            return None

        rec_model_path = os.path.join(v5_base, f'{rec_model_dir_name}_infer'
                                      if not rec_model_dir_name.endswith('_infer')
                                      else rec_model_dir_name)

        if not os.path.exists(rec_model_path):
            rec_model_path = os.path.join(v5_base, rec_model_dir_name)

        if not os.path.exists(rec_model_path):
            return None

        rec_model_path = self._ensure_ascii_model_dir(rec_model_path)
        rec_model_name = self._read_model_name_from_yaml(rec_model_path)
        return det_model_path, rec_model_path, det_model_name, rec_model_name
