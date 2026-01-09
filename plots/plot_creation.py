import matplotlib.pyplot as plt

# --- DATA ---
# Fixed: Added 4th label to match the 4 data points
attempts_labels = ['Baseline\n(Raw TF)', 'BM25\nOnly', 'BM25 +\nPageRank', 'Optimized\n(Pruning)']
execution_times = [8.0, 5.8, 6.1, 0.4]
p_at_10_scores = [14.0, 22.0, 33.5, 48.8]

# --- GRAPH 1: EXECUTION TIME ---
plt.figure(figsize=(8, 5))
# Highlight the final bar in green, others in skyblue
colors = ['skyblue', 'skyblue', 'skyblue', 'mediumseagreen']
bars = plt.bar(attempts_labels, execution_times, color=colors)

plt.axhline(y=4, color='red', linestyle='--', label='Max Latency Threshold (4s)')
plt.title('Execution Time per Experiment')
plt.ylabel('Time (seconds)')

# Fixed: Adjusted limit to show the highest value (8s) comfortably
plt.ylim(0, max(execution_times) + 1)
plt.legend()

for bar in bars:
    yval = bar.get_height()
    plt.text(bar.get_x() + bar.get_width()/2, yval + 0.1, f'{yval}s', ha='center', va='bottom', fontweight='bold')

plt.tight_layout()
plt.savefig('time_graph.png')
print("Saved time_graph.png")
plt.close()

# --- GRAPH 2: PRECISION @ 10 ---
plt.figure(figsize=(8, 5))
# Highlight the final bar in green, others in salmon
colors_p = ['salmon', 'salmon', 'salmon', 'mediumseagreen']
bars2 = plt.bar(attempts_labels, p_at_10_scores, color=colors_p)

plt.title('Precision at 10 (P@10) Improvement')
plt.ylabel('Score (%)')
plt.ylim(0, max(p_at_10_scores) + 10) # Add headroom for labels

for bar in bars2:
    yval = bar.get_height()
    plt.text(bar.get_x() + bar.get_width()/2, yval + 1, f'{yval}%', ha='center', va='bottom', fontweight='bold')

plt.tight_layout()
plt.savefig('p_at_10_graph.png')
print("Saved p_at_10_graph.png")