import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import numpy as np

PLOTS_DIR = "/Volumes/LaCie/OpenOBA/PLOTS/"

# http_accept = [373, 356, 348, 369, 366, 374]
# http_ignore = [210, 193, 193, 193, 191, 190]
# http_reject = [209, 208, 203, 189, 204, 180]

cookies_accept = [131, 111, 105, 118, 121, 130]
cookies_ignore = [47, 29, 31, 33, 33, 33]
cookies_reject = [40, 40, 39, 31, 41, 35]
cookies_overlap = [33, 27, 30, 29, 29, 27]

cookies_data = [cookies_accept, cookies_ignore, cookies_reject, cookies_overlap]


def plot_ads_boxplot(data, file_name, data_dir):
    # data = [accept, do_nothing, do_nothing_all, reject]
    box_colors = ["#228833", "#CCBB44", "#EE6677", "#808080"]

    # Create the plot with the specified size
    plt.figure(figsize=(15, 15))

    # Custom positions for the boxes
    positions = [1, 2, 3, 4]

    # Create the boxplot, hiding outlier points
    boxplot = plt.boxplot(
        data, positions=positions, patch_artist=True, showfliers=False, widths=0.3
    )

    # Customize the colors and hatches of the boxes
    for patch, color in zip(boxplot["boxes"], box_colors):
        patch.set_facecolor(color)

    # Customize the plot labels
    ylabel_msg = "# Unique Cookie Hosts"

    plt.ylabel(ylabel_msg, fontsize=35)
    plt.tick_params(axis="y", labelsize=32)
    plt.xticks(
        [1, 2, 3, 4], ["Accept All", "No Action", "Reject All", "Overlap"], fontsize=35
    )

    # Set y-axis to start from 0
    plt.ylim(bottom=0)  # Add this line
    # plt.xticks([1, 2, 3, 4], ["", "", "", ""], fontsize=25)

    # Add legend
    # accept_legend = mpatches.Patch(
    #     facecolor="#228833", edgecolor="black", label="Accept All"
    # )
    # no_action_legend = mpatches.Patch(
    #     facecolor="#CCBB44", edgecolor="black", label="No Action"
    # )
    # reject_legend = mpatches.Patch(
    #     facecolor="#EE6677", edgecolor="black", label="Reject All"
    # )
    # overlap_legend = mpatches.Patch(
    #     facecolor="#808080", edgecolor="black", label="Overlap"
    # )
    # plt.legend(
    #     handles=[accept_legend, no_action_legend, reject_legend, overlap_legend],
    #     fontsize=33,
    #     loc="upper right",
    # )

    # Reduce spacing between the boxes
    plt.subplots_adjust(left=0.15, right=0.85, top=0.85, bottom=0.15)

    # Save the plot as a PDF file
    plt.savefig(f"{data_dir}/boxplots/{file_name}.pdf", bbox_inches="tight")

    # Close the plot
    plt.close()


plot_ads_boxplot(cookies_data, "cookies_by_type", PLOTS_DIR)


# def create_boxplots(data, data_type, data_dir=PLOTS_DIR):
#     # data = [accept_providers, accept_all, do_nothing_providers, do_nothing_all, reject_providers, reject_all]
#     box_colors = ["#228833", "#228833", "#CCBB44", "#CCBB44", "#EE6677", "#EE6677"]
#     hatch_patterns = [None, "//", None, "//", None, "//"]

#     # Create the plot with the specified size
#     plt.figure(figsize=(15, 15))

#     # Custom positions for the boxes
#     positions = [1, 1.3, 2, 2.3, 3, 3.3]

#     # Create the boxplot, hiding outlier points
#     boxplot = plt.boxplot(
#         data, positions=positions, patch_artist=True, showfliers=False, widths=0.2
#     )

#     # Customize the colors and hatches of the boxes
#     for patch, color, hatch in zip(boxplot["boxes"], box_colors, hatch_patterns):
#         patch.set_facecolor(color)
#         if hatch is not None:
#             patch.set_hatch(hatch)

#     # Customize the plot labels
#     ylabel_msg = "# unique third-party tracking domains"

#     plt.ylabel(ylabel_msg, fontsize=33)
#     plt.tick_params(axis="y", labelsize=32)
#     plt.xticks([1.15, 2.15, 3.15], ["Accept", "No Action", "Reject"], fontsize=33)

#     # Add legend
#     no_pattern_patch = mpatches.Patch(
#         facecolor="white", edgecolor="black", label="HTTP Requests"
#     )
#     pattern_patch = mpatches.Patch(
#         facecolor="white", edgecolor="black", hatch="//", label="Cookies"
#     )
#     plt.legend(
#         handles=[no_pattern_patch, pattern_patch], fontsize=24, loc="upper right"
#     )

#     # Reduce spacing between the boxes
#     plt.subplots_adjust(left=0.15, right=0.85, top=0.85, bottom=0.15)

#     # Save the plot as a PDF file
#     plt.savefig(data_dir + "boxplots/" + data_type + ".pdf", bbox_inches="tight")

#     # Close the plot
#     plt.close()


# create_boxplots(data, "tracking_by_type")
