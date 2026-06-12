# -*- coding: utf-8 -*-
"""
モバカルネット「検査 → 入力済検査一覧」を巡回し、
検査会社＝「昭和メディカルサイエンス」のみ詳細ページ（?pid=exam_data&exam_id=◯）に遷移、
先頭のHTMLテーブルをCSVとして保存する実運用向けスクリプト（Windows + Chrome + Playwright）。

⚠️注意：
- 本スクリプトは Playwright の「永続コンテキスト」を使用して、
  既存の Chrome プロファイル（クライアント証明書登録済み）をそのまま使います。
- 個人情報の取扱いに配慮し、保存先はアクセス権のある暗号化ボリューム等を推奨。
- 初回は小さな範囲でテストし、問題がないことを確認してから全件巡回してください。

【事前準備】
  py -m venv venv
  venv\Scripts\activate
  pip install playwright
  playwright install

【実行】
  py mobacal_playwright_csv.py

【差し替えが必要な定数】
  - USER_DATA_DIR: 証明書入り Chrome ユーザープロファイルのパス
  - BASE_ORIGIN  : モバカルのオリジン（例: https://xxxxx.example.jp）
  - LOGIN_ID     : ログインID（ユーザー名）
  - LOGIN_PASSWORD: ログインパスワード
  - LIST_URL_PATTERN: 一覧URLパターン（offset を埋め込む）
  - SELECTORS: DOM セレクタと列番号（必要に応じて調整）

作者想定：初心者でも読めるように日本語コメントを多めに記載。
"""

from __future__ import annotations
import os
import re
import csv
import time
import random
from datetime import datetime
from urllib.parse import urljoin, urlparse, parse_qs
from typing import List, Dict, Optional, Tuple

from playwright.sync_api import sync_playwright, BrowserContext, Page, TimeoutError as PlaywrightTimeoutError

# ======== 変更することが多い定数（環境依存） ========
USER_DATA_DIR: str = r"C:\\Users\\hanamaruu96\\AppData\\Local\\Google\\Chrome\\User Data\\efault"
BASE_ORIGIN: str = "https://s2.movacal.net/24.4/"
LIST_URL_PATTERN: str = "?pid=exam&offset={offset}"
PAGE_SIZE: int = 20
COMPANY_ALLOWLIST: List[str] = ["昭和メディカルサイエンス"]
LOGIN_ID: str = "konishi"
LOGIN_PASSWORD: str = "konini"
REG_DATE_FROM: str = "2018-07-01"
REG_DATE_TO: str = "2019-07-31"
OUTPUT_DIR: str = "mobacal_exam_csv"
END_OFFSET_FALLBACK: int = 15000
WAIT_MIN: float = 1.0
WAIT_MAX: float = 2.0
WAIT_NAV_MIN: float = 2.0
WAIT_NAV_MAX: float = 3.0
APPEND_AUDIT_COLUMNS: bool = False
UTF8_WITH_BOM: bool = True
FORCE_SJIS_DECODE: bool = False
SAVE_ALL_TABLES: bool = True
DISABLE_DETAIL_ACCESS: bool = False
FAST_MODE: bool = True
PROGRESS_INTERVAL: int = 50
ULTRA_FAST_MODE: bool = True

SELECTORS = {
    "login_id_field": "input[name='login_id'], input[name='userid'], input[name='username'], input[type='text']",
    "login_password_field": "input[name='password'], input[type='password']",
    "login_button": "input[type='submit'], button[type='submit'], input[value*='ログイン'], button:has-text('ログイン')",
    "login_success_indicators": "a:has-text('ログアウト'), a:has-text('logout'), text=ようこそ, text=メニュー, .menu, .navigation",
    "table_list": "table.highlight-table",
    "table_list_rows": "table.highlight-table tbody tr",
    "pager_links": "a[href*=\"offset=\"]",
    "reg_date_cell_index": 1,
    "company_cell_index": 2,
    "file_cell_index": 3,
    "detail_first_table": "table",
    "detail_all_tables": "table",
    "exam_result_table": "table:has(th:has-text('検査項目')), table:has(th:has-text('結果')), table:has(th:has-text('基準値'))",
}

# ======== ここから下は通常変更不要 ========

def _sleep_random(min_s: float, max_s: float) -> None:
    time.sleep(random.uniform(min_s, max_s))


def _ts() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _ensure_dir(path: str) -> None:
    if not os.path.exists(path):
        os.makedirs(path, exist_ok=True)


def build_list_url(offset: int) -> str:
    rel = LIST_URL_PATTERN.format(offset=offset)
    return urljoin(BASE_ORIGIN, rel)


def is_company_allowed(name: str) -> bool:
    for allow in COMPANY_ALLOWLIST:
        if name == allow:
            return True
    return False


def extract_exam_id_from_href(href: str) -> Optional[str]:
    try:
        if href.startswith("/"):
            full = urljoin(BASE_ORIGIN, href)
        elif href.startswith("http"):
            full = href
        else:
            full = urljoin(BASE_ORIGIN, "/" + href)
        q = parse_qs(urlparse(full).query)
        vals = q.get("exam_id")
        if vals:
            return vals[0]
        m = re.search(r"exam_id=(\d+)", href)
        if m:
            return m.group(1)
    except Exception:
        pass
    return None


def parse_reg_date(text: str) -> Optional[datetime]:
    t = (text or "").strip()
    if not t:
        return None
    m = re.search(r"(\d{4})[/-](\d{1,2})[/-](\d{1,2})", t)
    if m:
        try:
            y, mo, d = int(m.group(1)), int(m.group(2)), int(m.group(3))
            return datetime(y, mo, d)
        except Exception:
            return None
    return None


def is_reg_date_in_range(reg_date_text: str) -> bool:
    reg_dt = parse_reg_date(reg_date_text)
    if reg_dt is None:
        return False
    try:
        dt_from = datetime.strptime(REG_DATE_FROM, "%Y-%m-%d")
        dt_to = datetime.strptime(REG_DATE_TO, "%Y-%m-%d")
    except ValueError:
        return False
    return dt_from <= reg_dt <= dt_to


# ======== [NEW] 登録日 → DSYYMMDDファイル名変換 ========

def reg_date_to_ds_stem(reg_date_text: str) -> Optional[str]:
    """登録日テキストから 'DSYYMMDD' 形式のステム（拡張子なし）を生成する。
    例: '2018/07/15 12:34' → 'DS180715'
    失敗時は None。
    """
    dt = parse_reg_date(reg_date_text)
    if dt is None:
        return None
    return "DS" + dt.strftime("%y%m%d")


def resolve_output_path(stem: str, ext: str = ".csv") -> str:
    """DS180715.csv のパスを返す。
    同名ファイルが既に存在する場合は DS180715_2.csv, DS180715_3.csv ... と連番を付ける。
    （_1 は使わず、2枚目から _2 とする。元ファイル名との視覚的区別を優先）
    """
    candidate = os.path.join(OUTPUT_DIR, stem + ext)
    if not os.path.exists(candidate):
        return candidate
    n = 2
    while True:
        candidate = os.path.join(OUTPUT_DIR, f"{stem}_{n}{ext}")
        if not os.path.exists(candidate):
            return candidate
        n += 1


# ======== ログイン ========

def perform_login(page: Page) -> bool:
    try:
        print(f"[{_ts()}] ログイン処理を開始します...")
        login_id_selector = SELECTORS["login_id_field"]
        password_selector = SELECTORS["login_password_field"]
        button_selector = SELECTORS["login_button"]
        try:
            id_field = page.locator(login_id_selector).first
            id_field.wait_for(timeout=10000)
            print(f"[{_ts()}] ログインIDフィールドを発見しました")
        except PlaywrightTimeoutError:
            print(f"[{_ts()}] ログインIDフィールドが見つかりません。既にログイン済みの可能性があります")
            return True
        try:
            password_field = page.locator(password_selector).first
            password_field.wait_for(timeout=5000)
        except PlaywrightTimeoutError:
            return False
        id_field.clear(); id_field.fill(LOGIN_ID); _sleep_random(0.5, 1.0)
        password_field.clear(); password_field.fill(LOGIN_PASSWORD); _sleep_random(0.5, 1.0)
        try:
            login_button = page.locator(button_selector).first
            login_button.wait_for(timeout=5000)
            login_button.click()
            current_url_before = page.url
            _sleep_random(3.0, 5.0)
            current_url_after = page.url
            if current_url_before != current_url_after:
                print(f"[{_ts()}] ログインに成功しました（URL変化を確認）")
                return True
            try:
                exam_url = urljoin(BASE_ORIGIN, "?pid=exam&offset=0")
                page.goto(exam_url, wait_until="domcontentloaded", timeout=30000)
                _sleep_random(2.0, 3.0)
                try:
                    if not page.locator(login_id_selector).first.is_visible(timeout=3000):
                        print(f"[{_ts()}] ログインに成功しました")
                        return True
                except:
                    return True
            except Exception as exam_error:
                pass
            print(f"[{_ts()}] ログイン状態不明のため処理を続行します")
            return True
        except PlaywrightTimeoutError:
            return False
    except Exception as e:
        print(f"[{_ts()}] ログイン処理中にエラー: {e}")
        return False


def detect_max_offset(page: Page) -> Optional[int]:
    try:
        links = page.locator(SELECTORS["pager_links"])
        count = links.count()
        max_offset = None
        for i in range(count):
            href = links.nth(i).get_attribute("href") or ""
            m = re.search(r"offset=(\d+)", href)
            if m:
                val = int(m.group(1))
                if (max_offset is None) or (val > max_offset):
                    max_offset = val
        return max_offset
    except Exception:
        return None


def parse_list_rows(page: Page) -> List[Dict[str, str]]:
    """一覧テーブルの各行から必要情報を取り出す。
    戻り値：[{company, file_text, href, exam_id, reg_date_text} ...]
    """
    items: List[Dict[str, str]] = []
    try:
        if page.is_closed():
            return items
        rows = page.locator(SELECTORS["table_list_rows"])
        row_count = rows.count()
    except Exception as e:
        print(f"[{_ts()}] テーブル行の取得中にエラー: {e}")
        return items

    for r in range(row_count):
        row = rows.nth(r)
        cells = row.locator("td, th")
        cell_count = cells.count()
        if cell_count == 0:
            continue
        try:
            reg_date_idx = SELECTORS["reg_date_cell_index"]
            company_idx  = SELECTORS["company_cell_index"]
            file_idx     = SELECTORS["file_cell_index"]
            if reg_date_idx >= cell_count or company_idx >= cell_count or file_idx >= cell_count:
                continue

            reg_date_text = cells.nth(reg_date_idx).inner_text().strip()
            if not is_reg_date_in_range(reg_date_text):
                continue

            company = cells.nth(company_idx).inner_text().strip()
            if not is_company_allowed(company):
                continue

            file_cell = cells.nth(file_idx)
            link = file_cell.locator("a").first
            href = link.get_attribute("href") or ""
            file_text = link.inner_text().strip() if href else ""
            exam_id = extract_exam_id_from_href(href) or ""
            if not href or not file_text or not exam_id:
                continue

            items.append({
                "company": company,
                "file_text": file_text,
                "href": href,
                "exam_id": exam_id,
                "reg_date_text": reg_date_text,  # [NEW] 登録日を保持
            })
        except Exception:
            continue
    return items


def save_table_as_csv_from_detail(page: Page, output_path: str, exam_id: str) -> Tuple[bool, str]:
    try:
        print(f"[{_ts()}] 詳細ページのテーブルを解析中...")
        try:
            if SAVE_ALL_TABLES:
                all_tables = page.locator(SELECTORS["detail_all_tables"])
                table_count = all_tables.count()
                print(f"[{_ts()}] {table_count} 個のテーブルが見つかりました")
            else:
                exam_tables = page.locator(SELECTORS["exam_result_table"])
                exam_table_count = exam_tables.count()
                if exam_table_count > 0:
                    all_tables = exam_tables
                    table_count = exam_table_count
                else:
                    all_tables = page.locator(SELECTORS["detail_first_table"])
                    table_count = all_tables.count()
            if table_count == 0:
                raise RuntimeError("no tables found on page")
        except Exception as table_error:
            raise RuntimeError(f"table search failed: {table_error}")

        encoding = "utf-8-sig" if UTF8_WITH_BOM else "utf-8"
        all_rows_data: List[List[str]] = []

        for table_idx in range(table_count):
            try:
                table = all_tables.nth(table_idx)
                table_html = table.inner_html(timeout=10000)
                rows = table.locator("tr")
                row_count = rows.count()
                print(f"[{_ts()}] テーブル {table_idx+1}: {row_count} 行")
            except Exception as e:
                print(f"[{_ts()}] テーブル {table_idx+1} の処理でエラー: {e}")
                continue

            if row_count == 0:
                continue
            if table_idx > 0:
                all_rows_data.append([])
                all_rows_data.append([f"=== テーブル {table_idx + 1} ==="])

            ultra_fast_success = False
            if ULTRA_FAST_MODE:
                try:
                    from bs4 import BeautifulSoup
                    soup = BeautifulSoup(table_html, 'html.parser')
                    html_rows = soup.find_all(['tr'])
                    max_cols = 0
                    for tr in html_rows:
                        max_cols = max(max_cols, len(tr.find_all(['td','th'])))
                    for row_idx, tr in enumerate(html_rows):
                        if row_idx % PROGRESS_INTERVAL == 0:
                            print(f"[{_ts()}] HTML解析: 行 {row_idx+1}/{len(html_rows)}")
                        cells = tr.find_all(['td','th'])
                        row_texts = [c.get_text(strip=True) for c in cells]
                        while len(row_texts) < max_cols:
                            row_texts.append("")
                        all_rows_data.append(row_texts)
                    ultra_fast_success = True
                except ImportError:
                    pass
                except Exception as e:
                    print(f"[{_ts()}] 超高速処理失敗: {e}")

            if FAST_MODE and not ultra_fast_success:
                max_cols = 0
                for i in range(min(10, row_count)):
                    try:
                        max_cols = max(max_cols, rows.nth(i).locator("th, td").count())
                    except:
                        continue
                for i in range(row_count):
                    if i % PROGRESS_INTERVAL == 0:
                        print(f"[{_ts()}] 行 {i+1}/{row_count}")
                    try:
                        cells = rows.nth(i).locator("th, td")
                        row_texts = []
                        for j in range(cells.count()):
                            try:
                                txt = cells.nth(j).inner_text().strip()
                                row_texts.append(txt)
                            except:
                                row_texts.append("")
                        while len(row_texts) < max_cols:
                            row_texts.append("")
                        all_rows_data.append(row_texts)
                    except Exception as e:
                        all_rows_data.append([""] * max_cols)

            elif not FAST_MODE and not ultra_fast_success:
                for i in range(row_count):
                    try:
                        cells = rows.nth(i).locator("th, td")
                        row_texts = [cells.nth(j).inner_text().strip() for j in range(cells.count())]
                        all_rows_data.append(row_texts)
                    except:
                        continue

        if APPEND_AUDIT_COLUMNS and all_rows_data:
            if all_rows_data[0]:
                all_rows_data[0] += ["exam_id", "retrieved_at", "source"]
            for i in range(1, len(all_rows_data)):
                if all_rows_data[i]:
                    all_rows_data[i] += [exam_id, _ts(), "html_dom"]

        with open(output_path, "w", newline="", encoding=encoding) as f:
            writer = csv.writer(f)
            for row_data in all_rows_data:
                safe_row = []
                for cell in (row_data or []):
                    if cell is None:
                        safe_row.append("")
                    else:
                        safe_row.append(str(cell).replace('\r','').replace('\n',' ').replace('\t',' '))
                writer.writerow(safe_row)

        print(f"[{_ts()}] {table_count} テーブル / {len(all_rows_data)} 行 → {output_path}")

        try:
            html_path = re.sub(r"\.csv$", "_debug.html", output_path, flags=re.IGNORECASE)
            with open(html_path, "w", encoding="utf-8") as f:
                f.write(page.content())
        except Exception as e:
            print(f"[{_ts()}] デバッグHTML保存エラー: {e}")

        return True, "html_dom"

    except Exception as e:
        print(f"[{_ts()}] CSV保存エラー: {e}")
        try:
            html_path = re.sub(r"\.csv$", "_fromhtml.html", output_path, flags=re.IGNORECASE)
            with open(html_path, "w", encoding="utf-8") as f:
                f.write(page.content())
            return False, "html_fallback"
        except:
            return False, "html_fallback"


# ======== [MODIFIED] ファイル名を DSYYMMDDに変換して保存 ========

def _build_output_path(item: Dict[str, str]) -> str:
    """item の登録日から DSYYMMDD.csv パスを決定する。
    登録日が解析できない場合はオリジナルの file_text をフォールバックとして使う。
    同名ファイルが既存なら連番（_2, _3 ...）を付ける。
    """
    stem = reg_date_to_ds_stem(item.get("reg_date_text", ""))
    if stem is None:
        # フォールバック: 元のファイル名そのまま（拡張子がなければ付ける）
        fallback = item["file_text"].strip()
        if not fallback.lower().endswith(".csv"):
            fallback += ".csv"
        return os.path.join(OUTPUT_DIR, fallback)
    return resolve_output_path(stem, ".csv")


def create_csv_from_list_info(item: Dict[str, str]) -> Tuple[bool, str]:
    """一覧ページの情報のみでCSVファイルを作成する（詳細ページにアクセスしない）"""
    exam_id = item["exam_id"].strip()
    out_path = _build_output_path(item)

    try:
        encoding = "utf-8-sig" if UTF8_WITH_BOM else "utf-8"
        with open(out_path, "w", newline="", encoding=encoding) as f:
            writer = csv.writer(f)
            writer.writerow(["検査会社", "元ファイル名", "検査ID", "登録日", "取得日時", "データソース", "備考"])
            writer.writerow([
                item["company"],
                item["file_text"],
                exam_id,
                item.get("reg_date_text", ""),
                _ts(),
                "list_page_only",
                "詳細ページアクセス無効化モード",
            ])
        print(f"[{_ts()}] 一覧情報のみでCSV作成: {out_path}")
        return True, f"[SAVE_LIST_ONLY] exam_id={exam_id} -> {os.path.basename(out_path)}"
    except Exception as e:
        return False, f"[ERROR_LIST_CSV] exam_id={exam_id} msg={e}"


def process_item(page: Page, item: Dict[str, str]) -> Tuple[bool, str]:
    """1レコードを処理：詳細へ遷移しCSV保存。"""
    exam_id = item["exam_id"].strip()
    href    = item["href"].strip()
    out_path = _build_output_path(item)

    # ---- URL構築 ----
    if href.startswith("/"):
        url = urljoin(BASE_ORIGIN, href)
    elif href.startswith("http"):
        url = href
    elif href.startswith("?"):
        url = BASE_ORIGIN + href
    else:
        url = urljoin(BASE_ORIGIN, href)

    list_url = page.url
    try:
        print(f"[{_ts()}] 詳細ページ遷移: {url}")
        page.goto(url, wait_until="domcontentloaded", timeout=30000)
        _sleep_random(WAIT_NAV_MIN, WAIT_NAV_MAX)

        body_text = page.locator("body").inner_text()
        if "二重ログイン" in body_text or "ログアウトしてください" in body_text:
            page.goto(list_url, wait_until="domcontentloaded", timeout=30000)
            return False, f"[ERROR:DOUBLE_LOGIN] exam_id={exam_id}"

        ok, src = save_table_as_csv_from_detail(page, out_path, exam_id)

        page.goto(list_url, wait_until="domcontentloaded", timeout=30000)
        _sleep_random(WAIT_NAV_MIN, WAIT_NAV_MAX)

        label = "[SAVE]" if ok else "[FALLBACK_HTML]"
        return True, f"{label} exam_id={exam_id} -> {os.path.basename(out_path)} source={src}"

    except PlaywrightTimeoutError:
        try: page.goto(list_url, wait_until="domcontentloaded", timeout=30000)
        except: pass
        return False, f"[ERROR:TIMEOUT] exam_id={exam_id} url={url}"
    except Exception as e:
        try: page.goto(list_url, wait_until="domcontentloaded", timeout=30000)
        except: pass
        return False, f"[ERROR] exam_id={exam_id} url={url} msg={e}"


def main() -> None:
    print(f"[{_ts()}] START")
    _ensure_dir(OUTPUT_DIR)

    with sync_playwright() as p:
        context = p.chromium.launch_persistent_context(
            user_data_dir=USER_DATA_DIR,
            headless=False,
            channel="chrome",
            args=[
                "--disable-blink-features=AutomationControlled",
                "--disable-dev-shm-usage",
                "--no-sandbox",
                "--disable-web-security",
                "--disable-features=VizDisplayCompositor"
            ],
            viewport={"width": 1280, "height": 900},
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        )

        page = context.new_page()
        try:
            page.goto(BASE_ORIGIN, wait_until="domcontentloaded", timeout=60000)
            _sleep_random(WAIT_NAV_MIN, WAIT_NAV_MAX)

            if not perform_login(page):
                print(f"[{_ts()}] ログインに失敗しました。終了します。")
                return

            offset = 0
            list_url = build_list_url(offset)
            max_retry = 2
            for retry in range(max_retry):
                try:
                    page.goto(list_url, wait_until="domcontentloaded", timeout=60000)
                    _sleep_random(WAIT_NAV_MIN, WAIT_NAV_MAX)
                    if page.locator(SELECTORS["table_list"]).count() > 0:
                        print(f"[{_ts()}] 検査ページに正常アクセス")
                        break
                    if page.locator(SELECTORS["login_id_field"]).first.is_visible(timeout=3000):
                        if not perform_login(page):
                            if retry == max_retry - 1:
                                print(f"[{_ts()}] 再ログイン失敗。終了します。")
                                return
                except Exception as e:
                    print(f"[{_ts()}] 検査ページ遷移エラー: {e}")
                    if retry == max_retry - 1:
                        return
            else:
                return

            max_offset = detect_max_offset(page)
            if max_offset is None:
                max_offset = END_OFFSET_FALLBACK
                print(f"[{_ts()}] pager 自動検出失敗。フォールバック END_OFFSET={max_offset}")
            else:
                print(f"[{_ts()}] pager 自動検出 END_OFFSET={max_offset}")

            total_saved = 0
            for offset in range(0, max_offset + 1, PAGE_SIZE):
                list_url = build_list_url(offset)
                try:
                    page.goto(list_url, wait_until="domcontentloaded", timeout=60000)
                except Exception as e:
                    print(f"[{_ts()}] [ERROR:NAV] offset={offset} msg={e}")
                    _sleep_random(WAIT_MIN, WAIT_MAX)
                    continue

                _sleep_random(WAIT_NAV_MIN, WAIT_NAV_MAX)

                try:
                    if page.is_closed():
                        break
                except:
                    break

                try:
                    items = parse_list_rows(page)
                    print(f"[{_ts()}] offset={offset} matched_rows={len(items)}")
                except Exception as e:
                    print(f"[{_ts()}] [ERROR:PARSE] offset={offset} msg={e}")
                    _sleep_random(WAIT_MIN, WAIT_MAX)
                    continue

                for it in items:
                    try:
                        if DISABLE_DETAIL_ACCESS:
                            saved, log = create_csv_from_list_info(it)
                        else:
                            saved, log = process_item(page, it)
                        print(f"[{_ts()}] {log}")
                        if saved:
                            total_saved += 1
                    except Exception as e:
                        print(f"[{_ts()}] [ERROR:PROCESS] exam_id={it.get('exam_id','?')} msg={e}")

                _sleep_random(WAIT_MIN, WAIT_MAX)

            print(f"[{_ts()}] DONE total_saved={total_saved}")

        finally:
            try: page.close()
            except: pass
            try: context.close()
            except: pass


if __name__ == "__main__":
    main()