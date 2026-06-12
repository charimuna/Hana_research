import matplotlib.pyplot as plt

# 共通設定
plt.rcParams['font.family'] = 'sans-serif'
plt.rcParams['pdf.fonttype'] = 42

def create_figure_1():
    """フローチャートの作成 [cite: 1-8]"""
    fig, ax = plt.subplots(figsize=(8, 10))
    ax.set_axis_off()
    
    box_style = dict(boxstyle='square,pad=0.5', fc='white', ec='black', lw=1)
    arrow_props = dict(arrowstyle='->', lw=1.5)

    # テキスト定義
    txt_total = "Total patients identified\n(n = 3,672)"
    txt_deceased = "Deceased patients\n(n = 1,879)"
    txt_not_inc = "Not included in the analysis (n = 1,793)\n• Alive: n = 798\n• Lost to follow-up: n = 953\n• Aged <20 years: n = 42"
    txt_excluded = "Excluded\n• Terminal cancer: n = 598"
    txt_final = "Final population for analysis\n(n = 1,281)"

    # 配置
    ax.text(0.5, 0.9, txt_total, ha='center', va='center', bbox=box_style)
    ax.annotate("", xy=(0.5, 0.8), xytext=(0.5, 0.86), arrowprops=arrow_props)
    
    ax.text(0.5, 0.75, txt_deceased, ha='center', va='center', bbox=box_style)
    ax.annotate("", xy=(0.75, 0.825), xytext=(0.5, 0.825), arrowprops=arrow_props)
    ax.text(0.77, 0.825, txt_not_inc, ha='left', va='center', fontsize=9, bbox=box_style)
    
    ax.annotate("", xy=(0.5, 0.62), xytext=(0.5, 0.70), arrowprops=arrow_props)
    ax.annotate("", xy=(0.75, 0.67), xytext=(0.5, 0.67), arrowprops=arrow_props)
    ax.text(0.77, 0.67, txt_excluded, ha='left', va='center', fontsize=9, bbox=box_style)
    
    ax.text(0.5, 0.57, txt_final, ha='center', va='center', weight='bold', bbox=box_style)

    plt.savefig('Figure1_Flowchart.pdf', format='pdf', bbox_inches='tight')
    plt.close()

def create_figure_2():
    """リスク群別棒グラフの作成 """
    labels = ['Low (0–1)', 'Intermediate (2–3)', 'High (4–6)']
    percentages = [12.9, 16.4, 28.0]
    errors = [4.0, 2.5, 5.0] 
    n_deaths = [24, 129, 86]
    n_total = [186, 788, 307]

    fig, ax = plt.subplots(figsize=(7, 6))
    bars = ax.bar(labels, percentages, yerr=errors, capsize=10, color='#E6E6E6', edgecolor='black', width=0.6)

    # 棒の上のパーセンテージ表示
    for i, bar in enumerate(bars):
        yval = bar.get_height()
        ax.text(bar.get_x() + bar.get_width()/2, yval + errors[i] + 1, f'{percentages[i]}%', ha='center', va='bottom')

    ax.set_ylabel('Unexpected death (%)', fontsize=12)
    ax.set_ylim(0, 40)
    ax.spines['top'].set_visible(False)
    ax.spines['right'].set_visible(False)

    # 修正: エラーの出やすい plt.table を避け、textで $n$ 数を配置
    y_base = -8
    ax.text(-0.8, y_base, 'Unexpected Deaths (n)', ha='left', va='center', fontsize=10)
    ax.text(-0.8, y_base - 3, 'Total Patients (n)', ha='left', va='center', fontsize=10)

    for i in range(len(labels)):
        ax.text(i, y_base, str(n_deaths[i]), ha='center', va='center', fontsize=10)
        ax.text(i, y_base - 3, str(n_total[i]), ha='center', va='center', fontsize=10)

    plt.savefig('Figure2_BarChart.pdf', format='pdf', bbox_inches='tight')
    plt.close()

def create_figure_3():
    """概念図の作成 [cite: 27-53]"""
    fig, ax = plt.subplots(figsize=(10, 8))
    ax.set_axis_off()

    def draw_box(x, y, text, title=False, facecolor='white'):
        weight = 'bold' if title else 'normal'
        ax.text(x, y, text, ha='center', va='top', wrap=True, fontsize=10, weight=weight,
                bbox=dict(boxstyle='round,pad=0.5', fc=facecolor, ec='black'))

    draw_box(0.25, 0.95, "1. Clinical Phenotypes\nAssociated with Unexpected Death", title=True, facecolor='#F2F2F2')
    phenotypes = "A. Apparent Clinical Stability\n(Age < 85, Preserved ADL, No dementia)\n\nB. Masked Physiological Decline\n(Enteral nutrition masking anorexia)\n\nC. Acute-risk comorbidity\n(Respiratory disease)"
    draw_box(0.25, 0.85, phenotypes)

    draw_box(0.75, 0.95, "2. Impaired Shared Recognition", title=True, facecolor='#F2F2F2')
    recognition = "• Perceptual gap between team/family\n• Limited communication\n• Prognostic uncertainty\n• Deferred ACP discussion"
    draw_box(0.75, 0.85, recognition)

    # 中央の強調
    ax.annotate("Prognostic Recognition Gap", xy=(0.5, 0.65), xytext=(0.5, 0.75),
                arrowprops=dict(arrowstyle='<->', color='red', lw=2),
                ha='center', fontsize=12, color='red', weight='bold')

    draw_box(0.5, 0.45, "3. Unexpected Death in Home Medical Care", title=True, facecolor='#F2F2F2')
    result = "• Family unpreparedness and distress\n• Abrupt event at home\n• Death perceived as 'unexpected'"
    draw_box(0.5, 0.35, result)

    plt.savefig('Figure3_Conceptual.pdf', format='pdf', bbox_inches='tight')
    plt.close()

# 実行
if __name__ == "__main__":
    create_figure_1()
    create_figure_2()
    create_figure_3()
    print("PDFs have been successfully generated.")