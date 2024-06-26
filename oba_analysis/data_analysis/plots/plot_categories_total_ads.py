import pandas as pd
import matplotlib.pyplot as plt
import numpy as np

PLOTS_DIR = "/Volumes/LaCie/OpenOBA/PLOTS/"


def create_plot_for_instance_categories_data(data, file_name=None, data_dir=PLOTS_DIR):
    df = data

    # Sorting the data by 'PercentageFromAllUniqueAds' and 'percentageFromAllTotalAds' to get rankings
    df_sorted_unique = df.sort_values(
        by="PercentageFromAllUniqueAds", ascending=False
    ).reset_index(drop=True)
    df_sorted_total = df.sort_values(
        by="percentageFromAllTotalAds", ascending=False
    ).reset_index(drop=True)

    # Creating a mapping from category to its rank based on 'NumAds'
    rank_map = {cat: rank for rank, cat in enumerate(df_sorted_total["Category"], 1)}

    # Selecting the top 15 based on unique ads and adding rank index to the Category label
    df_top15 = df_sorted_unique.head(15)
    df_top15["Category"] = df_top15["Category"].apply(
        lambda x: f"{x} ({df_top15[df_top15['Category'] == x].index[0] + 1} | {rank_map[x]})"
    )

    # Plotting the data
    fig, ax = plt.subplots(figsize=(12, 10))

    # Change color of bars depending on experiment name
    # if "accept" in file_name:
    # color = "#228833"
    # elif "reject" in file_name:
    # color = "#EE6677"
    # else:
    # yellow
    # color = "#CCBB44"
    # Leave only one color
    color = "#228833"

    # Adding bars for total ads and unique ads percentages
    bars = ax.barh(
        df_top15["Category"],
        df_top15["percentageFromAllTotalAds"],
        color=color,
        alpha=0.5,
        label="Ads Percentage",
    )
    unique_bars = ax.barh(
        df_top15["Category"],
        df_top15["PercentageFromAllUniqueAds"],
        color=color,
        alpha=1,
        label="Unique Ads Percentage",
    )

    # Adding labels and title
    ax.set_xlabel("Percentage of Unique Ads / Percentage of Total Ads", fontsize=14)
    plt.legend(fontsize=12)

    # Annotate percentage values on bars
    for unique_bar, total_bar in zip(unique_bars, bars):
        unique_width = unique_bar.get_width()
        total_width = total_bar.get_width()
        label_x_pos = unique_width + 2  # Slightly offset to avoid overlap
        ax.text(
            label_x_pos,
            unique_bar.get_y() + unique_bar.get_height() / 2,
            f"{unique_width:.2f} | {total_width:.2f}",
            va="center",
            fontsize=12,
        )

    # Customizing y-axis label colors for specific categories and increasing font size
    highlight = ["Shopping", "Style & Fashion"]
    for label in ax.get_yticklabels():
        label.set_fontsize(12)
        if "Shopping" in label.get_text() or "Style & Fashion" in label.get_text():
            label.set_color("red")

    # Reversing the y-axis to have the top rank at the top
    ax.invert_yaxis()

    # Adjusting the x-axis to show up to 60%
    ax.set_xlim(0, 60)

    # Saving the plot as a pdf
    plt.savefig(data_dir + f"{file_name}.pdf", bbox_inches="tight")


def create_plot_for_sessions_evolution(PLOTS_DIR):
    data = {
        "instance_name": ["OBA_accept", "OBA_ignore", "OBA_reject"],
        "NumAdsURLSession1": [12, 0, 0],
        "NumUniqueAdsURLSession1": [8, 0, 0],
        "NumAdsURLSession2": [61, 20, 14],
        "NumUniqueAdsURLSession2": [11, 1, 1],
        "NumAdsURLSession3": [48, 20, 24],
        "NumUniqueAdsURLSession3": [1, 1, 1],
        "NumAdsURLSession4": [36, 30, 42],
        "NumUniqueAdsURLSession4": [2, 1, 1],
        "NumAdsURLSession5": [34, 10, 40],
        "NumUniqueAdsURLSession5": [5, 1, 1],
        "NumAdsURLSession6": [20, 46, 30],
        "NumUniqueAdsURLSession6": [1, 4, 1],
    }

    df = pd.DataFrame(data)

    # Setup for the plot
    n_sessions = 6  # Number of sessions
    index = np.arange(n_sessions)  # X-axis index for groups
    bar_width = 0.25  # Width of the bars
    opacity = 0.2  # Opacity for the 'NumAds' bars

    fig, ax = plt.subplots(figsize=(12, 8))

    # Colors and labels
    colors = ["teal", "#FFDB58", "red"]
    labels = ["Accept", "Ignore", "Reject"]

    # Plotting
    for i in range(n_sessions):
        for j, instance in enumerate(df["instance_name"]):
            # Position for the bars in each group
            pos = index[i] + j * bar_width

            # Extracting data for the specific session
            num_ads = df.loc[j, f"NumAdsURLSession{i+1}"]
            num_unique_ads = df.loc[j, f"NumUniqueAdsURLSession{i+1}"]

            # # Plotting the total ads (translucent)
            # ax.bar(
            #     pos,
            #     num_ads,
            #     bar_width,
            #     alpha=opacity,
            #     color=colors[j],
            #     label=f"Total Ads - {labels[j]}" if i == 0 else "",
            # )
            # Plotting the unique ads (opaque)
            ax.bar(
                pos,
                num_unique_ads,
                bar_width,
                color=colors[j],
                label=f"{labels[j]}" if i == 0 else "",
            )

            # ax.text(
            #     pos,
            #     num_ads + 0.5,
            #     str(num_ads),
            #     ha="center",
            #     va="bottom",
            #     fontsize=8,
            #     fontweight="bold",
            # )
            ax.text(
                pos,
                num_unique_ads + 0.5,
                str(num_unique_ads),
                ha="center",
                va="bottom",
                fontsize=8,
                # color="white",
            )

    # Labeling and customizing
    ax.set_xlabel("Sessions")
    ax.set_ylabel("Number of Unique Ads")
    ax.set_xticks(index + bar_width)
    ax.set_xticklabels([f"Session {i+1}" for i in range(n_sessions)])
    ax.legend()

    # Show the plot
    plt.tight_layout()

    # Save the plot as a pdf
    plt.savefig(PLOTS_DIR + "unique_ads_by_session_evolution.pdf")

    # Now add the total ads to the plot
    fig, ax = plt.subplots(figsize=(12, 8))

    # Colors and labels
    colors = ["teal", "#FFDB58", "red"]
    labels = ["Accept", "Ignore", "Reject"]

    # Plotting
    for i in range(n_sessions):
        for j, instance in enumerate(df["instance_name"]):
            # Position for the bars in each group
            pos = index[i] + j * bar_width

            # Extracting data for the specific session
            num_ads = df.loc[j, f"NumAdsURLSession{i+1}"]
            # num_unique_ads = df.loc[j, f"NumUniqueAdsURLSession{i+1}"]

            # # Plotting the total ads (translucent)
            ax.bar(
                pos,
                num_ads,
                bar_width,
                color=colors[j],
                label=f"{labels[j]}" if i == 0 else "",
            )
            # Plotting the unique ads (opaque)
            # ax.bar(
            #     pos,
            #     num_unique_ads,
            #     bar_width,
            #     color=colors[j],
            #     label=f"Unique Ads - {labels[j]}" if i == 0 else "",
            # )

            ax.text(
                pos,
                num_ads + 0.5,
                str(num_ads),
                ha="center",
                va="bottom",
                fontsize=8,
                fontweight="bold",
            )
            # ax.text(
            #     pos,
            #     num_unique_ads + 0.5,
            #     str(num_unique_ads),
            #     ha="center",
            #     va="bottom",
            #     fontsize=8,
            #     # color="white",
            # )

    # Labeling and customizing
    ax.set_xlabel("Sessions")
    ax.set_ylabel("Number of Total Ads")
    ax.set_xticks(index + bar_width)
    ax.set_xticklabels([f"Session {i+1}" for i in range(n_sessions)])
    ax.legend()

    # Show the plot
    plt.tight_layout()

    # Save the plot as a pdf
    plt.savefig(PLOTS_DIR + "total_ads_by_session_evolution.pdf")

    fig, ax = plt.subplots(figsize=(12, 8))
    # Plotting
    for i in range(n_sessions):
        for j, instance in enumerate(df["instance_name"]):
            # Position for the bars in each group
            pos = index[i] + j * bar_width

            # Extracting data for the specific session
            num_ads = df.loc[j, f"NumAdsURLSession{i+1}"]
            num_unique_ads = df.loc[j, f"NumUniqueAdsURLSession{i+1}"]

            # Plotting the total ads (translucent)
            bars = ax.bar(
                pos,
                num_ads,
                bar_width,
                alpha=opacity,
                color=colors[j],
                label=f"Total Ads - {labels[j]}" if i == 0 else "",
            )
            # Plotting the unique ads (opaque)
            unique_bars = ax.bar(
                pos,
                num_unique_ads,
                bar_width,
                color=colors[j],
                label=f"Unique Ads - {labels[j]}" if i == 0 else "",
            )

            # Adding text annotation for each bar
            ax.text(
                pos,
                num_ads + 0.5,
                str(num_ads),
                ha="center",
                va="bottom",
                fontsize=8,
                fontweight="bold",
            )
            ax.text(
                pos,
                num_unique_ads + 0.5,
                str(num_unique_ads),
                ha="center",
                va="bottom",
                fontsize=8,
                # color="white",
            )

    # Labeling and customizing
    ax.set_xlabel("Sessions")
    ax.set_ylabel("Number of Ads")
    ax.set_title("Number of Ads by Session and Experiment Instance")
    ax.set_xticks(index + bar_width)
    ax.set_xticklabels([f"Session {i+1}" for i in range(n_sessions)])
    ax.legend()

    # Show the plot
    plt.tight_layout()

    # save the plot as a pdf
    plt.savefig(PLOTS_DIR + "both_ads_by_session_evolution.pdf")
