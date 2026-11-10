"""Draws the charts used in RESULTS.md. Numbers are copied from results/*.txt.
Run from the repo root:  python3 docs/make_charts.py
If you rerun a benchmark, update the numbers below to match the new file."""
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.ticker

BLUE, ORANGE, INK, MUTED, GRID = "#2a78d6", "#eb6834", "#0b0b0b", "#52514e", "#e6e5e1"
plt.rcParams.update({
    "figure.facecolor": "white", "axes.facecolor": "white", "savefig.facecolor": "white",
    "font.family": "DejaVu Sans", "font.size": 11, "text.color": INK,
    "axes.edgecolor": "#b5b4ae", "axes.labelcolor": MUTED, "xtick.color": MUTED, "ytick.color": MUTED,
    "axes.spines.top": False, "axes.spines.right": False,
})

def style(ax):
    ax.grid(axis="y", color=GRID, linewidth=1)
    ax.set_axisbelow(True)

def save(fig, name):
    fig.tight_layout()
    fig.savefig(f"docs/charts/{name}.png", dpi=160)
    plt.close(fig)

# 1. throughput (results/throughput.txt)
w = [1, 2, 4, 8, 16, 32]
jps = [2268, 2941, 3778, 4394, 4577, 4285]
fig, ax = plt.subplots(figsize=(7, 3.8))
ax.plot(range(len(w)), jps, color=BLUE, linewidth=2, marker="o", markersize=8, markeredgecolor="white", markeredgewidth=2)
for i, v in enumerate(jps):
    ax.annotate(f"{v:,}", (i, v), textcoords="offset points", xytext=(0, 10), ha="center", color=INK, fontsize=10)
ax.set_xticks(range(len(w))); ax.set_xticklabels(w)
ax.set_ylim(0, 5500); ax.set_xlabel("worker processes (4 slots each)"); ax.set_ylabel("jobs per second")
ax.set_title("Throughput, 20,000 no-op jobs (median of 3)", loc="left", fontsize=12, color=INK)
style(ax); save(fig, "throughput")

# 2. wait time vs arrival rate (results/latency.txt, latency-poll20-full.txt)
rates = ["500", "1,000", "2,000", "3,000"]
p99_200 = [181.4, 160.4, 195.0, 18.6]
p99_20 = [17.1, 12.6, 2.1, 10.6]
p50_200 = [50.2, 25.9, 0.8, 1.9]
p50_20 = [3.7, 0.8, 0.8, 1.8]
fig, axes = plt.subplots(1, 2, figsize=(9, 3.8), sharey=True)
x = range(len(rates)); bw = 0.38
for ax, (a, b, t) in zip(axes, [(p50_200, p50_20, "p50"), (p99_200, p99_20, "p99")]):
    ax.bar([i - bw/2 - 0.01 for i in x], a, bw, color=BLUE, label="200 ms poll")
    ax.bar([i + bw/2 + 0.01 for i in x], b, bw, color=ORANGE, label="20 ms poll")
    for i, (u, v) in enumerate(zip(a, b)):
        ax.text(i - bw/2 - 0.01, u + 3, f"{u:g}", ha="center", fontsize=8.5, color=MUTED)
        ax.text(i + bw/2 + 0.01, v + 3, f"{v:g}", ha="center", fontsize=8.5, color=MUTED)
    ax.set_xticks(list(x)); ax.set_xticklabels(rates)
    ax.set_xlabel("jobs per second arriving"); ax.set_title(f"{t} wait before start", loc="left", fontsize=12)
    ax.set_ylim(0, 225); style(ax)
axes[0].set_ylabel("milliseconds")
axes[0].legend(frameon=False, loc="upper right")
save(fig, "latency")

# 3. recovery (results/recovery.txt): median, with min-max range
leases = ["1 s", "3 s", "10 s"]
med = [1.05, 2.87, 9.35]; lo = [0.77, 2.23, 7.03]; hi = [1.75, 4.14, 10.41]
fig, ax = plt.subplots(figsize=(7, 3.6))
ax.bar(range(3), med, 0.5, color=BLUE)
ax.errorbar(range(3), med, yerr=[[m - l for m, l in zip(med, lo)], [h - m for h, m in zip(hi, med)]],
            fmt="none", ecolor=INK, capsize=5, linewidth=1.2)
for i, m in enumerate(med):
    ax.text(i + 0.3, m, f"{m} s", va="center", fontsize=10)
ax.set_xticks(range(3)); ax.set_xticklabels(leases); ax.set_xlim(-0.6, 2.9)
ax.set_xlabel("lease length"); ax.set_ylabel("seconds from kill -9 to restart")
ax.set_title("Recovery time, 20 kills per lease (bar = median, line = min to max)", loc="left", fontsize=11.5)
style(ax); save(fig, "recovery")

# 4. claims (results/claims.txt)
names = ["select, then update", "FOR UPDATE", "FOR UPDATE\nSKIP LOCKED"]
c = [1112, 4438, 15473]
fig, ax = plt.subplots(figsize=(7, 3.6))
ax.bar(range(3), c, 0.5, color=BLUE)
for i, v in enumerate(c):
    ax.text(i, v + 300, f"{v:,}", ha="center", fontsize=10)
ax.text(0, 2500, "102,921\ndouble claims", ha="center", fontsize=9, color=MUTED)
ax.set_xticks(range(3)); ax.set_xticklabels(names); ax.set_ylim(0, 18000)
ax.set_ylabel("jobs claimed per second")
ax.yaxis.set_major_formatter(matplotlib.ticker.FuncFormatter(lambda v, _: f"{int(v):,}"))
ax.set_title("Claim strategies, 16 claimers, 20,000 jobs", loc="left", fontsize=12)
style(ax); save(fig, "claims")

# 5. retry herd (results/herd.txt)
att = [1, 2, 3, 4, 5, 6, 8, 10]
peaks = [10000, 10000, 5026, 2553, 1294, 666, 187, 90]
fig, ax = plt.subplots(figsize=(7, 3.8))
ax.plot(range(len(att)), [10000]*len(att), color=ORANGE, linewidth=2, marker="o", markersize=8, markeredgecolor="white", markeredgewidth=2, label="no jitter")
ax.plot(range(len(att)), peaks, color=BLUE, linewidth=2, marker="o", markersize=8, markeredgecolor="white", markeredgewidth=2, label="with jitter")
ax.set_yscale("log"); ax.set_ylim(50, 20000)
ax.set_xticks(range(len(att))); ax.set_xticklabels(att)
ax.set_xlabel("retry number"); ax.set_ylabel("most retries in one second (log scale)")
ax.set_title("Retry herd, 10,000 jobs failing at once (simulation)", loc="left", fontsize=12)
ax.annotate("90", (7, 90), textcoords="offset points", xytext=(0, -16), ha="center", fontsize=9, color=MUTED)
ax.legend(frameon=False, loc="lower left"); style(ax); save(fig, "herd")