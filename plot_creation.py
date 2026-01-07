import matplotlib.pyplot as plt

# --- DATA ---
# Renamed variable to 'execution_times' to avoid conflicts
attempts_data = ['Baseline (Raw TF)', 'BM25 Only', 'Final (BM25 + PageRank)']
execution_times = [3.2, 3.8, 2.9]
p_at_10_scores = [14, 22, 33.5]  # Your new scores

# --- GRAPH 1: EXECUTION TIME ---
plt.figure(figsize=(6, 4))
bars = plt.bar(attempts_data, execution_times, color='skyblue')

plt.axhline(y=4, color='red', linestyle='--', label='4s Threshold')
plt.title('Execution Time per Attempt')
plt.ylabel('Time (seconds)')
plt.ylim(0, 5) # This is fine for times like 3.2, 3.8
plt.legend()

for bar in bars:
    yval = bar.get_height()
    plt.text(bar.get_x() + bar.get_width()/2, yval + 0.1, f'{yval}s', ha='center', va='bottom')

plt.savefig('time_graph.png')
print("Saved time_graph.png")
plt.close()

# --- GRAPH 2: P@10 (FIXED) ---
plt.figure(figsize=(6, 4))
bars2 = plt.bar(attempts_data, p_at_10_scores, color='salmon')

plt.title('Precision at 10 (P@10)')
plt.ylabel('Score')

# FIX: Removed the 0-1.1 limit. Now it autoscales.
# Optional: Set it to slightly above your max score (e.g., 40) just to look nice
plt.ylim(0, max(p_at_10_scores) + 5)

for bar in bars2:
    yval = bar.get_height()
    plt.text(bar.get_x() + bar.get_width()/2, yval + 0.5, f'{yval}', ha='center', va='bottom')

plt.savefig('p_at_10_graph.png')
print("Saved p_at_10_graph.png")