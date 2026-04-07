# generate_mobacal_list.py
# -*- coding: utf-8 -*-
"""
Playwright + existing Chrome profile to scrape PDF document list from Movacal
and generate mobacal_pdf_list.tsv automatically.

This script logs into the Movacal system and extracts the list of PDF documents
with their URLs, then saves them in TSV format (YYYYMMDD_ID<TAB>URL).
"""

import asyncio
import os
import re
import unicodedata
from datetime import date
from pathlib import Path
from playwright.async_api import async_playwright

# =================== CONFIG ===================
USER_DATA_DIR = r"C:\Users\hanamaruu96\AppData\Local\Google\Chrome\User Data"
PROFILE_NAME  = "Default"  # or "Profile 1", "Profile 2" etc.
OUT_TSV       = r"C:\Users\hanamaruu96\Desktop\database\PDFinstall\mobacal_pdf_list.tsv"
MOVACAL_BASE  = "https://s2.movacal.net/24.5/"
MAX_PAGES     = 0  # 最大ページ数（0=全ページ取得）
# 取得期間（開始日は固定、終了日は実行当日）
START_DATE    = "2015-04-01"
END_DATE      = date.today().strftime("%Y-%m-%d")
# ドキュメント一覧ページのURL（カルテ記録一覧ページ）
# docs=19 でPDF文書のみ表示、num_per_page=1000 で1000件表示
DOCS_LIST_URL = (
    "https://s2.movacal.net/24.5/?"
    "diag_type_csv=%E8%A8%BA%E7%99%82%E3%82%BF%E3%82%A4%E3%83%97&"
    "act_date_csv=%E8%A8%BA%E7%99%82%E6%97%A5%E6%99%82&"
    "flag_comment_csv=%E3%83%95%E3%83%A9%E3%82%B0%E5%82%99%E8%80%83&"
    "patient_id_csv=ID&"
    "patient_name_csv=%E6%82%A3%E8%80%85%E6%B0%8F%E5%90%8D&"
    "patient_facility_csv=%E6%82%A3%E8%80%85%E6%96%BD%E8%A8%AD&"
    "karte_content_csv=%E3%82%AB%E3%83%AB%E3%83%86%E5%86%85%E5%AE%B9&"
    "doctor_name_csv=%E5%8C%BB%E5%B8%AB%E5%90%8D&"
    "end_time_csv=%E7%B5%82%E4%BA%86%E6%99%82%E9%96%93&"
    "kyotaku_comment_csv=%E5%B1%85%E5%AE%85%E7%99%82%E9%A4%8A%E7%AE%A1%E7%90%86%E6%8C%87%E5%B0%8E&"
    "family_comment_csv=%E3%81%94%E5%AE%B6%E6%97%8F%E3%81%B8%E3%81%AE%E9%80%A3%E7%B5%A1%E4%BA%8B%E9%A0%85&"
    "bt_csv=%E4%BD%93%E6%B8%A9&bp_csv=%E8%A1%80%E5%9C%A7&pr_csv=%E8%84%88%E6%8B%8D&"
    "seifusei_data3_csv=%E8%84%88%E6%8B%8D%E6%95%B4%E4%B8%8D%E6%95%B4&"
    "o2_csv=SPO2&o2comment_csv=SPO2%E5%82%99%E8%80%83&"
    "kokyusu_csv=%E5%91%BC%E5%90%B8%E6%95%B0&"
    "seifusei_data4_csv=%E5%91%BC%E5%90%B8%E6%95%B0%E6%95%B4%E4%B8%8D%E6%95%B4&"
    "height_csv=%E8%BA%AB%E9%95%B7&weight_csv=%E4%BD%93%E9%87%8D&"
    f"start_date={START_DATE}&end_date={END_DATE}&"
    "record_type=2&keyword=&injection=&medicine=&medicine_hidden=&medicine_inner=&medicine_inner_hidden=&"
    "act=&act_hidden=&orca_act=&orca_act_hidden=&insurance_check=0&public_insurance_check=0&doctor_name=&"
    "diag_dept=&diag_type=&karteflag=&tagsAnd=0&diag_type_except=&destination_type=&docs=19&docs_status=&"
    "docs_order=&station_name=&facility=0&facility_hospital=&type=&status=&doc_category=&num_per_page=1000&"
    "offset_for_csv=0&pid=record_index&all2=0&order_type=act_date&desc_type=0"
)
# ==============================================

# 日付とID抽出用の正規表現
DATE_ID_RE = re.compile(r'(\d{8})_(\d+)')
DOCS_ID_RE = re.compile(r'docs_id=(\d+)')
PATIENT_ID_RE = re.compile(r'patient_id=(\d+)')
 
def extract_info_from_url(url: str):
    """URLから docs_id と patient_id を抽出"""
    docs_match = DOCS_ID_RE.search(url)
    patient_match = PATIENT_ID_RE.search(url)
    
    docs_id = docs_match.group(1) if docs_match else None
    patient_id = patient_match.group(1) if patient_match else None
    
    return docs_id, patient_id


def extract_date_id_from_text(text: str):
    """テキストから YYYYMMDD_ID 形式を抽出"""
    match = DATE_ID_RE.search(text)
    if match:
        return f"{match.group(1)}_{match.group(2)}"
    return None


def convert_date_to_yyyymmdd(date_str: str, default_year: int = None) -> str:
    """
    日付文字列をYYYYMMDD形式に変換
    例: '3/28(金)' -> '20250328'
    例: '2024/12/24(火)' -> '20241224'
    """
    if not date_str:
        return ''
    # 全角→半角などの正規化
    try:
        date_str = unicodedata.normalize('NFKC', date_str).strip()
    except Exception:
        pass
    
    # 年が省略されている場合は実行年を使用
    if default_year is None:
        default_year = date.today().year
    
    # パターン1: YYYY/MM/DD 形式（年が含まれている）
    match = re.match(r'(\d{4})/(\d{1,2})/(\d{1,2})', date_str)
    if match:
        year = int(match.group(1))
        month = int(match.group(2))
        day = int(match.group(3))
        return f"{year:04d}{month:02d}{day:02d}"

    # パターン1b: YYYY-MM-DD or YYYY.MM.DD
    match = re.match(r'(\d{4})[-.](\d{1,2})[-.](\d{1,2})', date_str)
    if match:
        year = int(match.group(1))
        month = int(match.group(2))
        day = int(match.group(3))
        return f"{year:04d}{month:02d}{day:02d}"

    # パターン1c: YYYY年M月D日
    match = re.match(r'(\d{4})年(\d{1,2})月(\d{1,2})日', date_str)
    if match:
        year = int(match.group(1))
        month = int(match.group(2))
        day = int(match.group(3))
        return f"{year:04d}{month:02d}{day:02d}"
    
    # パターン2: M/DD(曜) 形式（年が省略されている = 今年）
    # 例: '12/25(木)' → '20251225'
    match = re.match(r'(\d{1,2})/(\d{1,2})', date_str)
    if match:
        month = int(match.group(1))
        day = int(match.group(2))
        year = default_year
        # 月の妥当性チェック
        if 1 <= month <= 12 and 1 <= day <= 31:
            return f"{year:04d}{month:02d}{day:02d}"
    
    return ''


async def main():
    async with async_playwright() as p:
        # Chrome プロファイルを使用してブラウザを起動
        profile_dir = os.path.join(USER_DATA_DIR, PROFILE_NAME)
        ctx = await p.chromium.launch_persistent_context(
            user_data_dir=profile_dir,
            channel="chrome",
            headless=False,
            accept_downloads=False,
            args=["--no-default-browser-check", "--no-first-run"],
        )
        
        page = await ctx.new_page()
        page.set_default_navigation_timeout(180000)
        page.set_default_timeout(60000)
        
        print(f"[INFO] ブラウザを起動しました")
        print(f"[INFO] {MOVACAL_BASE} にアクセスします")
        
        # ログインページへ遷移
        try:
            await page.goto(MOVACAL_BASE, wait_until="domcontentloaded")
        except Exception as e:
            print(f"[WARN] ナビゲーション警告: {e}")
        
        # ログイン確認
        input("\n[USER] ログインを完了してから Enter キーを押してください...")
        
        print(f"\n[INFO] ドキュメント一覧ページ ({DOCS_LIST_URL}) にアクセスします")
        
        # ドキュメント一覧ページへ遷移
        try:
            await page.goto(DOCS_LIST_URL, wait_until="domcontentloaded")
            await page.wait_for_timeout(2000)  # ページが完全にロードされるまで待機
        except Exception as e:
            print(f"[ERROR] ドキュメント一覧ページへのアクセスに失敗: {e}")
            print("[INFO] 現在のページからPDFリンクを抽出します")
        
        input("\n[USER] ドキュメント一覧が表示されていることを確認して Enter キーを押してください...")
        
        # 全ページからPDFリンクを収集
        print("\n[INFO] PDFリンクを収集中...")
        all_pdf_links = []
        page_num = 1
        
        while True:
            print(f"\n[INFO] ページ {page_num} を処理中...")
            
            # 現在のページ（全フレーム含む）からPDFリンクを収集
            pdf_links = []
            frames = page.frames
            print(f"[DEBUG] フレーム数: {len(frames)}")
            for idx, frame in enumerate(frames, start=1):
                try:
                    links_in_frame = await frame.evaluate("""
                        () => {
                            const links = [];
                            const norm = (s) => {
                                try { return (s || '').normalize('NFKC').trim(); } catch(e) { return (s || '').trim(); }
                            };
                            // PDFリンク候補を広めに拾う
                            const elements = document.querySelectorAll(
                                'a[href*="docs_disp-pdf"], a[href*=".pdf"], a[href*="pdf="]'
                            );
                            elements.forEach(el => {
                                const href = el.href;
                                if (!href) return;
                                const text = norm(el.textContent || '');
                                const title = norm(el.getAttribute('title') || '');

                                // 親行から日付とIDを柔軟に抽出
                                let dateText = '';
                                let patientId = '';
                                const row = el.closest('tr');
                                if (row) {
                                    const cells = Array.from(row.querySelectorAll('td'));
                                    const rawTexts = cells.map(td => norm(td.textContent || ''));
                                    const rowText = rawTexts.join(' ');

                                    // 優先: Column 1 (登録日) から日付を取得
                                    if (cells[1]) {
                                        const c = norm(cells[1].textContent || '');
                                        // パターン: M/DD(曜) 例: "12/25(木)"
                                        let m = c.match(/^(\d{1,2})\/(\d{1,2})/);
                                        if (m) {
                                            dateText = `${m[1]}/${m[2]}`;
                                        } else {
                                            // パターン: YYYY/MM/DD
                                            m = c.match(/(\d{4})[\/.-](\d{1,2})[\/.-](\d{1,2})/);
                                            if (m) {
                                                dateText = `${m[1]}/${m[2]}/${m[3]}`;
                                            } else {
                                                // パターン: YYYY年M月D日
                                                m = c.match(/(\d{4})年(\d{1,2})月(\d{1,2})日/);
                                                if (m) {
                                                    dateText = `${m[1]}/${m[2]}/${m[3]}`;
                                                }
                                            }
                                        }
                                    }

                                    // 優先: Column 3 (文書ID) から ID を取得
                                    if (cells[3]) {
                                        const c = norm(cells[3].textContent || '');
                                        const idMatch = c.match(/(\d{5,7})/);
                                        if (idMatch) {
                                            patientId = idMatch[1];
                                        }
                                    }

                                    // フォールバック: 行全体から ID を抽出
                                    if (!patientId) {
                                        for (let i = 0; i < rawTexts.length; i++) {
                                            const idMatch = rawTexts[i].match(/^(\d{5,7})$/);
                                            if (idMatch) {
                                                patientId = idMatch[1];
                                                break;
                                            }
                                        }
                                    }
                                }

                                // URLから patient_id 抽出（行で見つからない場合）
                                if (!patientId) {
                                    const matchPid = href.match(/patient_id=(\d+)/);
                                    if (matchPid) {
                                        patientId = matchPid[1];
                                    }
                                }

                                // URLから docs_id 抽出（最終フォールバック用）
                                let docsId = '';
                                const matchDid = href.match(/docs_id=(\d+)/);
                                if (matchDid) {
                                    docsId = matchDid[1];
                                }

                                links.push({
                                    url: href,
                                    text,
                                    title,
                                    dateText,
                                    patientId,
                                    docsId
                                });
                            });
                            return links;
                        }
                    """)
                    print(f"[DEBUG] フレーム {idx}: {len(links_in_frame)} 件")
                    pdf_links.extend(links_in_frame or [])
                except Exception as e:
                    print(f"[DEBUG] フレーム {idx} 取得失敗: {e}")
            
            print(f"[INFO] ページ {page_num}: {len(pdf_links)} 件のPDFリンクを検出")
            
            # デバッグ: 最初の数件の抽出結果を表示
            if page_num == 1 and len(pdf_links) > 0:
                print(f"[DEBUG] 最初の3件のデータ抽出結果:")
                for i, link in enumerate(pdf_links[:3], 1):
                    print(f"  {i}. 日付='{link.get('dateText', '')}' / ID='{link.get('patientId', '')}' / URL={link['url'][:80]}")
            
            all_pdf_links.extend(pdf_links)
            
            # 「次へ」ボタンの存在確認とクリック
            # 次ページボタンの存在確認（全フレーム）
            next_button_exists = False
            for frame in frames:
                try:
                    found = await frame.evaluate("""
                        () => {
                            const candidates = [];
                            candidates.push(...document.querySelectorAll('input[type="button"], input[type="submit"], button, a'));
                            const isNextLike = (el) => {
                                const v = (el.value || el.textContent || '').trim();
                                return v === '>>' || v === '>' || v.includes('次へ') || v.toLowerCase().includes('next');
                            };
                            const nextButton = candidates.find(el => isNextLike(el) && !el.disabled);
                            return !!nextButton;
                        }
                    """)
                    if found:
                        next_button_exists = True
                        break
                except Exception:
                    continue
            
            if not next_button_exists:
                print(f"[INFO] 次ページボタンが見つかりません。最終ページに到達しました")
                break
            
            if MAX_PAGES > 0 and page_num >= MAX_PAGES:
                print(f"[INFO] 最大ページ数 ({MAX_PAGES}) に到達しました")
                break
            
            # 次ページへ移動（全フレームのいずれかでクリック）
            try:
                clicked = False
                for frame in frames:
                    try:
                        success = await frame.evaluate("""
                            () => {
                                const candidates = [];
                                candidates.push(...document.querySelectorAll('input[type="button"], input[type="submit"], button, a'));
                                const isNextLike = (el) => {
                                    const v = (el.value || el.textContent || '').trim();
                                    return v === '>>' || v === '>' || v.includes('次へ') || v.toLowerCase().includes('next');
                                };
                                const nextEl = candidates.find(el => isNextLike(el) && !el.disabled);
                                if (nextEl) { nextEl.click(); return true; }
                                return false;
                            }
                        """)
                        if success:
                            clicked = True
                            break
                    except Exception:
                        continue
                if not clicked:
                    raise Exception("次ページのクリックに失敗（要素未検出）")
                # ページ読み込み待機
                await page.wait_for_timeout(2000)
                # テーブルが更新されるまで待機
                try:
                    waited = False
                    for frame in page.frames:
                        try:
                            await frame.wait_for_selector('a[href*="docs_disp-pdf"], a[href*=".pdf"], a[href*="pdf="]', timeout=8000)
                            waited = True
                            break
                        except Exception:
                            continue
                    if not waited:
                        await page.wait_for_timeout(2000)
                except Exception:
                    pass
                page_num += 1
            except Exception as e:
                print(f"[WARN] 次ページへの移動に失敗: {e}")
                break
        
        pdf_links = all_pdf_links
        print(f"\n[INFO] 合計 {len(pdf_links)} 件のPDFリンクを検出しました（{page_num} ページ）")
        
        if len(pdf_links) == 0:
            print("\n[WARN] PDFリンクが見つかりませんでした")
        
        # TSVデータを生成（仕様A: YYYYMMDD_ID<TAB>URL）
        tsv_lines = []
        seen_urls = set()
        
        for item in pdf_links:
            url = item['url']
            if url in seen_urls:
                continue
            seen_urls.add(url)
            
            identifier = None
            
            # 優先1: テーブル Column 1（登録日）+ Column 3（文書ID）を使用（最優先）
            if item.get('dateText') and item.get('patientId'):
                date_part = convert_date_to_yyyymmdd(item['dateText'])
                if date_part:
                    identifier = f"{date_part}_{item['patientId']}"
                    # 不要: print文削除（ログの削減）
            
            # 優先2: Column 3（文書ID）のみが取得できた場合（日付は今年で補完）
            if not identifier and item.get('patientId'):
                # 日付が取得できない場合は今年の当日を推定
                default_date = date.today().strftime("%Y%m%d")
                identifier = f"{default_date}_{item['patientId']}"
            
            # 優先3: URLからdocs_idを抽出（フォールバック）
            if not identifier:
                docs_id, patient_id = extract_info_from_url(url)
                if docs_id:
                    identifier = docs_id
            
            # 優先4: テキストまたはタイトルから YYYYMMDD_ID を抽出
            if not identifier and item['text']:
                identifier = extract_date_id_from_text(item['text'])
            
            if not identifier and item['title']:
                identifier = extract_date_id_from_text(item['title'])
            
            # 最終フォールバック: ページ内抽出済みの docsId を使用
            if not identifier and item.get('docsId'):
                identifier = item['docsId']

            if identifier:
                tsv_lines.append(f"{identifier}\t{url}")
            else:
                print(f"[WARN] 識別子を抽出できませんでした: 日付='{item.get('dateText', '')}' / ID='{item.get('patientId', '')}' -> {url[:100]}")
        
        # TSVファイルに書き込み
        if tsv_lines:
            Path(OUT_TSV).parent.mkdir(parents=True, exist_ok=True)
            with open(OUT_TSV, 'w', encoding='utf-8') as f:
                for line in tsv_lines:
                    f.write(line + '\n')
            
            print(f"\n[SUCCESS] {len(tsv_lines)} 件のエントリを {OUT_TSV} に保存しました")
            print(f"[INFO] 最初の5行:")
            for i, line in enumerate(tsv_lines[:5], 1):
                print(f"  {i}. {line[:120]}")
        else:
            print("\n[ERROR] TSVに書き込むデータがありません")
        
        await ctx.close()
        print("\n[INFO] 完了しました")


if __name__ == "__main__":
    asyncio.run(main())
