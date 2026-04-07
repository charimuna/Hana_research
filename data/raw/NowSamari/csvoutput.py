import pandas as pd
import tkinter as tk
from tkinter import filedialog, messagebox
import os

def filter_csv_columns(input_file, output_file):
    """
    CSVファイルを読み込み、指定した列のみを残して保存する
    
    Parameters:
    input_file (str): 入力CSVファイルのパス
    output_file (str): 出力CSVファイルのパス
    """
    # 残したい列のリスト（Excel列名形式）
    columns_to_keep = ['A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'K', 'M', 'N', 
                       'Q', 'R', 'AV', 'AW', 'AX', 'BG', 'BI', 'CF', 'CG', 'CI', 'CJ', 'CK', 
                       'CP', 'CQ', 'CR']
    
    # Excel列名を0始まりのインデックスに変換
    def excel_col_to_index(col):
        """Excel列名(A, B, AA等)を0始まりのインデックスに変換"""
        index = 0
        for char in col:
            index = index * 26 + (ord(char) - ord('A') + 1)
        return index - 1
    
    # 列インデックスのリストを作成
    column_indices = [excel_col_to_index(col) for col in columns_to_keep]
    
    # Q列とR列、M列、K列のインデックス（フィルタリング用）
    q_col_index = excel_col_to_index('Q')
    r_col_index = excel_col_to_index('R')
    m_col_index = excel_col_to_index('M')
    k_col_index = excel_col_to_index('K')
    
    # CSVファイルを読み込み（エンコーディング自動判定）
    print(f"CSVファイルを読み込んでいます: {input_file}")
    encodings = ['utf-8', 'shift_jis', 'cp932', 'utf-8-sig']
    df = None
    
    for enc in encodings:
        try:
            df = pd.read_csv(input_file, header=None, encoding=enc)
            print(f"エンコーディング: {enc}")
            break
        except:
            continue
    
    if df is None:
        raise Exception("ファイルを読み込めませんでした。エンコーディングを確認してください。")
    
    print(f"元のデータ: {df.shape[0]}行 × {df.shape[1]}列")
    
    # Q列（元データ）で「外来」と空欄の行を除外（1行目のヘッダーは保持）
    if q_col_index < df.shape[1]:
        # 1行目（インデックス0）はヘッダーとして保持
        header_row = df.iloc[[0]]
        data_rows = df.iloc[1:]
        
        # Q列の値を確認して、「外来」または空欄でない行のみを残す
        q_column = data_rows.iloc[:, q_col_index].astype(str)
        mask = ~((q_column == '外来') | (q_column == '') | (q_column == 'nan') | (q_column.isna()))
        filtered_data = data_rows[mask]
        
        # ヘッダーとデータを結合
        df = pd.concat([header_row, filtered_data], ignore_index=True)
        print(f"Q列フィルタ後: {df.shape[0]}行（ヘッダー含む）")
    
    # R列（元データ）で「外来」「はなまるクリニック職員」の行を除外（空欄は残す）
    if r_col_index < df.shape[1]:
        # 1行目（インデックス0）はヘッダーとして保持
        header_row = df.iloc[[0]]
        data_rows = df.iloc[1:]
        
        # R列の値を確認して、「外来」「はなまるクリニック職員」でない行のみを残す
        r_column = data_rows.iloc[:, r_col_index].astype(str)
        mask = ~((r_column == '外来') | (r_column == 'はなまるクリニック職員'))
        filtered_data = data_rows[mask]
        
        # ヘッダーとデータを結合
        df = pd.concat([header_row, filtered_data], ignore_index=True)
        print(f"R列フィルタ後: {df.shape[0]}行（ヘッダー含む）")

    # M/K列条件でフィルタ
    # 条件: Mが空欄の行を削除。さらに、Mが記載あり かつ Kが空欄の行も削除。
    # つまり、残すのは「MもKも空欄でない」行のみ。
    if (m_col_index < df.shape[1]) and (k_col_index < df.shape[1]):
        header_row = df.iloc[[0]]
        data_rows = df.iloc[1:]

        m_raw = data_rows.iloc[:, m_col_index]
        k_raw = data_rows.iloc[:, k_col_index]

        m_str = m_raw.astype(str).str.strip()
        k_str = k_raw.astype(str).str.strip()

        m_blank = m_raw.isna() | (m_str == '') | (m_str.str.lower() == 'nan')
        k_blank = k_raw.isna() | (k_str == '') | (k_str.str.lower() == 'nan')

        keep_mask = (~m_blank) & (~k_blank)
        filtered_data = data_rows[keep_mask]

        df = pd.concat([header_row, filtered_data], ignore_index=True)
        print(f"M/K列フィルタ後: {df.shape[0]}行（ヘッダー含む）")
    
    # 指定した列のみを選択（列が存在する場合のみ）
    valid_indices = [idx for idx in column_indices if idx < df.shape[1]]
    filtered_df = df.iloc[:, valid_indices]
    
    print(f"フィルタ後のデータ: {filtered_df.shape[0]}行 × {filtered_df.shape[1]}列")
    
    # A列（フィルタ後の最初の列=インデックス0）で昇順ソート
    # 1行目（ヘッダー）を保持したままソート
    header_row = filtered_df.iloc[[0]]
    data_rows = filtered_df.iloc[1:]
    
    # 数値として認識されない場合に備えて、数値変換を試みる
    try:
        data_rows.iloc[:, 0] = pd.to_numeric(data_rows.iloc[:, 0], errors='coerce')
    except:
        pass
    
    data_rows_sorted = data_rows.sort_values(by=data_rows.columns[0])
    filtered_df = pd.concat([header_row, data_rows_sorted], ignore_index=True)
    print(f"A列（1列目）で昇順ソート完了（1行目ヘッダー保持）")
    
    # 結果を保存（UTF-8で保存）
    filtered_df.to_csv(output_file, index=False, header=False, encoding='utf-8-sig')
    print(f"保存完了: {output_file}")
    return True

def select_and_process_csv():
    """GUIでCSVファイルを選択して処理"""
    # tkinterのルートウィンドウを非表示
    root = tk.Tk()
    root.withdraw()
    
    # CSVファイルを選択
    input_file = filedialog.askopenfilename(
        title="処理するCSVファイルを選択してください",
        filetypes=[("CSVファイル", "*.csv"), ("すべてのファイル", "*.*")]
    )
    
    if not input_file:
        print("ファイルが選択されませんでした。")
        return
    
    # 出力ファイル名を生成（元のファイル名に "_filtered" を追加）
    base_name = os.path.splitext(input_file)[0]
    output_file = f"{base_name}_filtered.csv"
    
    try:
        # 処理実行
        success = filter_csv_columns(input_file, output_file)
        if success:
            messagebox.showinfo("完了", f"処理が完了しました！\n保存先: {output_file}")
    except Exception as e:
        messagebox.showerror("エラー", f"エラーが発生しました:\n{str(e)}")
        print(f"エラー詳細: {e}")

if __name__ == "__main__":
    # GUIでファイル選択して処理
    select_and_process_csv()
