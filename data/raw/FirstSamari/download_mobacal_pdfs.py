# download_mobacal_pdfs.py
# -*- coding: utf-8 -*-
"""
Playwright + existing Chrome profile (with client cert) to download PDFs
and save using smart filename parsing.

Robust to:
- TSV encoding variants (UTF-8/UTF-8-SIG/SJIS/UTF-16)
- Space-separated rows without tabs
- Inline PDF viewers and embedded <embed>/<object>/<iframe>
- Partial/HTML saves (validates %PDF ... %%EOF)

Supported TSV formats (tab-separated or space + URL):
1) filename.pdf <TAB> url
2) YYYYMMDD_ID   <TAB> url
3) YYYYMMDD <TAB> ID <TAB> url
4) ID <TAB> url
5) Fallback: auto-detect an 8-digit date + >=3-digit ID in the row
"""

import asyncio
import os
import re
from glob import glob
from pathlib import Path
from urllib.parse import urljoin
from base64 import b64decode
from playwright.async_api import async_playwright

# =================== CONFIG ===================
USER_DATA_DIR = r"C:\Users\hanamaruu96\AppData\Local\Google\Chrome\User Data\Default"
LIST_PATH     = r"C:\Users\hanamaruu96\Desktop\database\PDFinstall\mobacal_pdf_list.tsv"
OUT_DIR       = r"C:\Users\hanamaruu96\Desktop\database\PDFinstall\out_pdfs"
BASE_ORIGIN   = "https://s2.movacal.net/24.5/"
# ==============================================

ENABLE_LOGIN_PROMPT = True
LOGIN_LANDING_URL   = "https://s2.movacal.net/24.4/"

# Regex (NOTE: no double-escape here)
DATE_RE = re.compile(r"(?<!\d)(\d{8})(?!\d)")   # 8 digits like 20251030
ID_RE   = re.compile(r"(?<!\d)(\d{3,})(?!\d)")  # 3+ digits
URL_RE  = re.compile(r"https?://\S+")


def sanitize_filename(name: str) -> str:
    name = name.strip().replace("\u3000", " ")
    name = re.sub(r'[\\/:*?"<>|]', "_", name)
    if not name.lower().endswith(".pdf"):
        name += ".pdf"
    return name


def resolve_tsv_files(path: str):
    if os.path.isdir(path):
        files = sorted(glob(os.path.join(path, "*.tsv")))
        if not files:
            raise FileNotFoundError(f"No TSV under: {path}")
        return files
    if not os.path.isfile(path):
        raise FileNotFoundError(f"TSV not found: {path}")
    return [path]


def iter_tsv_lines(tsv_path: str):
    """Yield lines with robust encoding fallback."""
    encs = ['utf-8', 'utf-8-sig', 'cp932', 'shift_jis', 'utf-16', 'utf-16-le', 'utf-16-be']
    for enc in encs:
        try:
            with open(tsv_path, 'r', encoding=enc, errors='strict') as f:
                for raw in f:
                    yield raw.rstrip('\r\n')
            return
        except Exception:
            continue
    # Last resort
    with open(tsv_path, 'r', encoding='utf-8', errors='ignore') as f:
        for raw in f:
            yield raw.rstrip('\r\n')


def _split_date_id_token(token: str):
    """Parse 'YYYYMMDD_ID' or 'YYYYMMDD-ID' or 'YYYYMMDD ID' into (date, id)."""
    t = token.strip()
    m = re.match(r"^(\d{8})[ _\-](\d{3,})$", t)
    if m:
        return m.group(1), m.group(2)
    return None, None


def parse_tsv_line(line: str):
    """Return (filename, url) or None. Tolerant to spaces, full-width spaces, and missing tabs."""
    if line is None:
        return None
    line_norm = line.replace('\u3000', ' ').strip()
    if not line_norm:
        return None

    # Prefer TAB; if not present, detect URL and split
    if '\t' in line_norm:
        parts = [p.strip() for p in line_norm.split('\t') if p.strip()]
    else:
        m = URL_RE.search(line_norm)
        if m:
            left = line_norm[:m.start()].strip()
            url = m.group(0)
            parts = [left, url]
        else:
            parts = [p.strip() for p in re.split(r"\s+", line_norm) if p.strip()]
            if len(parts) >= 2 and parts[-1].startswith(('http://','https://')):
                parts = [' '.join(parts[:-1]), parts[-1]]

    if len(parts) < 2:
        return None

    # 1) explicit filename
    if parts[0].lower().endswith('.pdf'):
        return sanitize_filename(parts[0]), parts[1]

    # 2) YYYYMMDD_ID
    d2, i2 = _split_date_id_token(parts[0])
    if d2 and i2:
        return sanitize_filename(f"{d2}_{i2}.pdf"), parts[1]

    # 3) date id url
    if len(parts) >= 3 and DATE_RE.fullmatch(parts[0] or '') and ID_RE.fullmatch(parts[1] or ''):
        return sanitize_filename(f"{parts[0]}_{parts[1]}.pdf"), parts[2]

    # 4) id url
    if ID_RE.fullmatch(parts[0] or ''):
        return sanitize_filename(f"{parts[0]}.pdf"), parts[1]

    # 5) auto-detect within the row
    date = None
    for tok in parts:
        m = DATE_RE.search(tok)
        if m:
            date = m.group(1)
            break

    nums = []
    for tok in parts:
        for m in re.finditer(r"\d{3,}", tok):
            nums.append(m.group(0))

    idnum = None
    if nums:
        for n in reversed(nums):
            if n != date:
                idnum = n
                break
        if not idnum:
            idnum = nums[-1]

    if date and idnum:
        url = parts[1] if parts[1].startswith(("http://", "https://", "./", "/?")) else parts[-1]
        return sanitize_filename(f"{date}_{idnum}.pdf"), url

    return None


def first_url_from_tsv(tsv: str):
    for line in iter_tsv_lines(tsv):
        parsed = parse_tsv_line(line)
        if parsed:
            _, url = parsed
            return url
    return None


async def main():
    Path(OUT_DIR).mkdir(parents=True, exist_ok=True)
    tsv_files = resolve_tsv_files(LIST_PATH)

    async with async_playwright() as p:
        ctx = await p.chromium.launch_persistent_context(
            user_data_dir=USER_DATA_DIR,
            channel="chrome",
            headless=False,
            accept_downloads=True,
            args=["--no-default-browser-check", "--no-first-run", "--disable-popup-blocking"],
        )
        page = await ctx.new_page()
        page.set_default_navigation_timeout(180000)
        page.set_default_timeout(60000)

        if ENABLE_LOGIN_PROMPT:
            try:
                print("[AUTH] Opening", LOGIN_LANDING_URL)
                await page.goto(LOGIN_LANDING_URL, wait_until="domcontentloaded")
            except Exception:
                pass
            input("ログイン完了後 Enter ...")
            test = first_url_from_tsv(tsv_files[0])
            if test:
                try:
                    print("[AUTH] Test", test)
                    await page.goto(test, wait_until="domcontentloaded")
                except Exception:
                    pass

        # ------ Helpers ------
        def is_valid_pdf(bin_data: bytes) -> bool:
            if not bin_data or len(bin_data) < 1024:
                return False
            if not bin_data.startswith(b"%PDF"):
                return False
            tail = bin_data[-2048:]
            return b"%%EOF" in tail

        async def try_inline_response(url: str, dest: Path) -> bool:
            try:
                resp = await page.goto(url, wait_until="domcontentloaded")
                if not resp:
                    return False
                ctype = (resp.headers or {}).get("content-type", "").lower()
                body = await resp.body()
                if ("application/pdf" in ctype) or body.startswith(b"%PDF"):
                    if is_valid_pdf(body):
                        dest.write_bytes(body)
                        print(f"[OK] {dest.name} body")
                        return True
                return False
            except Exception as e:
                print(f"[DBG] inline failed: {e}")
                return False

        async def try_download_event(url: str, dest: Path) -> bool:
            try:
                async with page.expect_download(timeout=120000) as dl:
                    await page.evaluate(
                        "(u)=>{const a=document.createElement('a');a.href=u;a.download='x.pdf';document.body.appendChild(a);a.click();a.remove();}",
                        url,
                    )
                d = await dl.value
                await d.save_as(dest)
                if dest.exists() and is_valid_pdf(dest.read_bytes()):
                    print(f"[OK] {dest.name} event")
                    return True
                return False
            except Exception as e:
                print(f"[DBG] download-event failed: {e}")
                return False

        async def try_dom_embeds(url: str, dest: Path) -> bool:
            try:
                await page.goto(url, wait_until="domcontentloaded")
                embed_src = await page.evaluate(
                    """
                    () => {
                      const pick = (sel) => { const el = document.querySelector(sel); return el && (el.src || el.data || el.getAttribute('src') || el.getAttribute('data')) };
                      return pick('embed[type="application/pdf"]') ||
                             pick('object[type="application/pdf"]') ||
                             pick('iframe[src*=".pdf"], iframe[src*="pdf="]');
                    }
                    """
                )
                if not embed_src:
                    embed_src = await page.evaluate("() => (document.querySelector('a[href*=.pdf], a[href*=docs_disp-pdf]') || {}).href || ''")
                if not embed_src:
                    return False
                if not (embed_src.startswith('http://') or embed_src.startswith('https://')):
                    from urllib.parse import urljoin as _uj
                    embed_src = _uj(url, embed_src)
                b64 = await page.evaluate(
                    """async (u) => { const r = await fetch(u, { credentials: 'include' }); if(!r.ok) throw new Error('HTTP '+r.status); const b = new Uint8Array(await r.arrayBuffer()); let s=''; for (let i=0;i<b.length;i++) s += String.fromCharCode(b[i]); return btoa(s); }""",
                    embed_src,
                )
                data = b64decode(b64)
                if is_valid_pdf(data):
                    dest.write_bytes(data)
                    print(f"[OK] {dest.name} embed")
                    return True
                return False
            except Exception as e:
                print(f"[DBG] dom-embed failed: {e}")
                return False

        async def save_pdf(filename: str, raw_url: str):
            url = raw_url.strip()
            if not (url.startswith("http://") or url.startswith("https://")):
                url = urljoin(BASE_ORIGIN, url)
            dest = Path(OUT_DIR) / sanitize_filename(filename)
            if dest.exists() and dest.stat().st_size > 0 and is_valid_pdf(dest.read_bytes()):
                print(f"[SKIP] {dest.name}")
                return
            print(f"[START] {dest.name}")

            for attempt in range(1, 4):
                ok = await try_inline_response(url, dest)
                if not ok:
                    ok = await try_download_event(url, dest)
                if not ok:
                    ok = await try_dom_embeds(url, dest)
                if not ok:
                    try:
                        b64 = await page.evaluate(
                            """async (u)=>{const r=await fetch(u,{credentials:'include'});if(!r.ok)throw new Error('HTTP '+r.status);const b=new Uint8Array(await r.arrayBuffer());let s='';for(let i=0;i<b.length;i++)s+=String.fromCharCode(b[i]);return btoa(s);}""",
                            url,
                        )
                        data = b64decode(b64)
                        if is_valid_pdf(data):
                            dest.write_bytes(data)
                            print(f"[OK] {dest.name} fetch")
                            return
                    except Exception as e:
                        print(f"[DBG] fetch-fallback failed: {e}")
                if ok:
                    return
                print(f"[RETRY] {dest.name} attempt {attempt} failed")
            print(f"[ERR] {dest.name} : saved file appears corrupted or not a PDF")

        for tsv in tsv_files:
            print(f"[READ] {tsv}")
            seen = []
            parsed_count = 0
            total_count = 0
            for line in iter_tsv_lines(tsv):
                total_count += 1
                if len(seen) < 3:
                    seen.append(line[:200])
                parsed = parse_tsv_line(line)
                if not parsed:
                    continue
                parsed_count += 1
                filename, url = parsed
                await save_pdf(filename, url)
            if parsed_count == 0:
                print(f"[WARN] No valid rows parsed from {tsv}. First lines sample:")
                for i, s in enumerate(seen, 1):
                    print(f"  L{i}: {s}")

        await ctx.close()


if __name__ == "__main__":
    asyncio.run(main())
