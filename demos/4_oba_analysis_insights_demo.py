from oba.oba_analysis import OBAAnalysis

# OBAQuantifier demo
oba_analysis = OBAAnalysis(
    experiment_name="test_style_and_fashion_experiment_accept",
    experiment_category="Style & Fashion",
)
organized_data = oba_analysis.fetch_control_site_ads_breakdown()
oba_analysis.generate_markdown_tables(organized_data)
aggregated_data = oba_analysis.fetch_aggregated_ads_breakdown()
oba_analysis.generate_aggregated_markdown_table(aggregated_data)
oba_analysis.visualize_ob_advertising_evolution_by_visit()
