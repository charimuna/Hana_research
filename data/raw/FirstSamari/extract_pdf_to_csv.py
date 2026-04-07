import os
import csv
import pdfplumber
from pathlib import Path

# PDFファイルが格納されているディレクトリ
pdf_dir = Path("out_pdfs")

# 出力するCSVファイル名
output_csv = "pdf_summary.csv"


def _format_date_from_yyyymmdd(s: str) -> str:
    """
    ファイル名の8桁日付（YYYYMMDD）を YYYY-MM-DD に整形。
    パースできない場合はそのまま返す。
    """
    if isinstance(s, str) and len(s) == 8 and s.isdigit():
        return f"{s[0:4]}-{s[4:6]}-{s[6:8]}"
    return s


def extract_pdf_info(pdf_path):
    """
    PDFファイルからテキストを抽出し、情報を返す
    
    Args:
        pdf_path: PDFファイルのパス
    
    Returns:
        tuple: (患者ID, 作成日, 書類名, 内容)
    """
    # ファイル名から患者IDと作成日を抽出
    filename = pdf_path.stem  # 拡張子を除いたファイル名
    parts = filename.split('_')
    
    if len(parts) >= 2:
        date = parts[0]  # 例: 20150403
        patient_id = parts[1]  # 例: 150016
    else:
        # ファイル名の形式が想定と異なる場合
        date = ""
        patient_id = filename
    
    # PDFからテキストを抽出
    try:
        with pdfplumber.open(pdf_path) as pdf:
            # 全ページのテキストを結合
            full_text = ""
            for page in pdf.pages:
                text = page.extract_text()
                if text:
                    full_text += text + "\n"
            
            # テキストを行に分割
            lines = full_text.strip().split('\n')
            
            # 書類名（1行目）と内容（2行目以降）に分割
            document_name = lines[0] if len(lines) > 0 else ""
            content = '\n'.join(lines[1:]) if len(lines) > 1 else ""
            
            # 作成日を日付として解釈されやすい形式に整形（YYYY-MM-DD）
            date_fmt = _format_date_from_yyyymmdd(date)
            return (patient_id, date_fmt, document_name, content)
    
    except Exception as e:
        print(f"エラー: {pdf_path.name} の処理中にエラーが発生しました: {e}")
        date_fmt = _format_date_from_yyyymmdd(date)
        return (patient_id, date_fmt, "", "")


def main():
    """
    メイン処理: PDFファイルを読み込んでCSVに出力
    """
    # PDFディレクトリの存在確認
    if not pdf_dir.exists():
        print(f"エラー: {pdf_dir} が見つかりません")
        print(f"現在のディレクトリ: {Path.cwd()}")
        return
    
    # PDFファイルのリストを取得（ソート済み）
    pdf_files = sorted(pdf_dir.glob("*.pdf"))
    
    if not pdf_files:
        print(f"エラー: {pdf_dir} にPDFファイルが見つかりません")
        return
    
    print(f"{len(pdf_files)}個のPDFファイルを処理します...")
    print(f"出力先: {output_csv}")
    print("-" * 50)
    
    # CSVファイルに書き込み
    with open(output_csv, 'w', newline='', encoding='utf-8-sig') as csvfile:
        writer = csv.writer(csvfile)
        
        # ヘッダー行を書き込み
        writer.writerow(['患者ID', '作成日', '書類名', '内容'])
        
        # 各PDFファイルを処理
        for i, pdf_file in enumerate(pdf_files, 1):
            print(f"処理中 ({i}/{len(pdf_files)}): {pdf_file.name}")
            
            # PDFから情報を抽出
            patient_id, date, document_name, content = extract_pdf_info(pdf_file)
            
            # CSVに1行書き込み
            writer.writerow([patient_id, date, document_name, content])
    
    print("-" * 50)
    print(f"✅ 完了: {output_csv} に出力しました")
    print(f"📊 処理件数: {len(pdf_files)}件")


if __name__ == "__main__":
    main()
