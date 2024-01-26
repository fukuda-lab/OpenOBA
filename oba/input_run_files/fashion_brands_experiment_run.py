API_KEY = "GhU39K7bdfvdxRlcnEkT"
SECRET_KEY = "ZwnCzHIpw08DF10Fmz5c"
credentials = {"api_key": API_KEY, "secret_key": SECRET_KEY}


# FASHION EXPERIMENT COOKIE BANNER UK VPN
fashion_uk_accept_cookies = {
    "experiment_name": "fashion_uk_accept_cookies",
    "fresh_experiment": True,
    "use_custom_pages": True,
    "cookie_banner_action": 1,
    "custom_pages_params": {
        "categorize_pages": False,  # If True we load the TrainingPagesHandler with the custom_pages_list=custom_pages_list, under custom_list=True, list_id={experiment_name}.
        "custom_pages_list": [  # List of URLs provided by the user. This list is not saved in the {experiment_name}_config.json, since it has to first be categorized and then accessed through the TrainingPagesHandler().get_training_pages_by_category() method.
            "https://www.canterbury.com/mens-ireland-team-polo-shirt-blue/13976088.html",
            "https://www.adidas.co.uk/samba-og-shoes/B75806.html?af_channel=Shopping_Search&af_reengagement_window=30d&c=GS-UK-Categories-Branded-High%20Stock-ROI&cm_mmc=AdieSEM_PLA_Google-_-GS-UK-Categories-Branded-High%20Stock-ROI-_-Branded%20High%20Stock%20-%20unisex-_-PRODUCT_GROUP&cm_mmca1=UK&cm_mmca2=&ds_agid=58700007370013582&gad_source=1&gclid=EAIaIQobChMIyqj8qP3DggMVR1ZyCh1C5wGBEAQYASABEgIiofD_BwE&gclsrc=aw.ds&is_retargeting=true&pid=googleadwords_temp",
            "https://www.clarks.com/en-gb/badell-top/26173418-p?gclid=CjwKCAiA0syqBhBxEiwAeNx9NwR4dG0Y3BwAygtvDqvzT_XWxnOFxvJAP6scRPCbV_JhuT731x4S6RoCxn0QAvD_BwE&gclsrc=aw.ds",
            "https://www.marksandspencer.com/3pk-regular-cotton-blend-long-sleeve-shirts/p/clp60546193#intid=pid_pg1pip48g4r2c3",
            "https://www.levi.com/GB/en_GB/clothing/men/501-original-jeans/p/005013383?camp=SHB_Levis_UK_EN_GOO_Performance_Max&gad=1&gclid=EAIaIQobChMI7ayWrILEggMV2VBHAR2jMAq-EAQYCCABEgKPbPD_BwE&gclsrc=aw.ds",
            "https://www.thenorthface.co.uk/shop/en/tnf-gb/mens-lhotse-down-jacket-3y23?variationId=N14&gclid=EAIaIQobChMIje3awYLEggMVrfPICh0ixQXoEAQYBSABEgINefD_BwE&gclsrc=aw.ds",
            "https://www.timberland.co.uk/shop/en/tbl-uk/new/timberland-premium-6-inch-boot-for-men-in-grey-a62bh033",
            "https://uk.puma.com/uk/en/pd/palermo-lth-unisex-sneakers/396464?swatch=01&size=0290&gad_source=1&gclid=EAIaIQobChMI9MXf8IjEggMVE-3ICh1E_g63EAQYAiABEgKoCfD_BwE",
            "https://www.lacoste.com/gb/lacoste/men/clothing/sweatshirts/paris-jacquard-monogram-zipped-sweatshirt/3617070338383.html?wiz_source=google&wiz_medium=cpc&wiz_campaign=17865838514&gclid=CjwKCAiA0syqBhBxEiwAeNx9N1OY6ASWjS17cXqqt0oeld07kV6XQ0wxHYXgOvaPcPba72MIQpEknhoCCvQQAvD_BwE",
            "https://www.ralphlauren.co.uk/en/custom-fit-plaid-oxford-shirt-650468.html?dwvar650468_colorname=6134%20Red%2FGreen%20Multi&cgid=men-clothing-casual-shirts&webcat=men%2Fclothing%2Fmen%20clothing%20casual%20shirts#webcat=men%7Cclothing%7Cmen-clothing-casual-shirts&start=1&cgid=men-clothing-casual-shirts",
            "https://www.zara.com/uk/en/hoodie-p00761350.html?v1=311297480&v2=2299536",
            "https://www2.hm.com/en_gb/productpage.1171208002.html",
            "https://www.gap.co.uk/style/ls413782/k69973#k69973",
            "https://www.oakley.com/en-gb/product/FOA404861?variant=193517897154",
            "https://aldoshoes.co.uk/collections/mens-shoes-trainers/products/lonespec-black-13614539",
            "https://uk.tommy.com/1985-collection-regular-fit-shorts-mw0mw23563rbl",
            "https://www.g-star.com/en_gb/shop/men/jeans/51001-c910-c778",
            "https://www.calvinklein.co.uk/3-pack-boxer-briefs-micro-stretch-wicking-000nb2570aub1",
            "https://www.asics.com/gb/en-gb/gel-nimbus-25/p/1011B547-004.html",
            "https://www.uniqlo.com/uk/en/product/flannel-easy-patterned-ankle-length-trousers-460277.html",
        ],
    },
}
# FASHION EXPERIMENT COOKIE BANNER UK VPN
fashion_uk_reject_cookies = {
    "experiment_name": "fashion_uk_reject_cookies",
    "fresh_experiment": True,
    "use_custom_pages": True,
    "cookie_banner_action": 2,
    "custom_pages_params": {
        "categorize_pages": False,  # If True we load the TrainingPagesHandler with the custom_pages_list=custom_pages_list, under custom_list=True, list_id={experiment_name}.
        "custom_pages_list": [  # List of URLs provided by the user. This list is not saved in the {experiment_name}_config.json, since it has to first be categorized and then accessed through the TrainingPagesHandler().get_training_pages_by_category() method.
            "https://www.canterbury.com/mens-ireland-team-polo-shirt-blue/13976088.html",
            "https://www.adidas.co.uk/samba-og-shoes/B75806.html?af_channel=Shopping_Search&af_reengagement_window=30d&c=GS-UK-Categories-Branded-High%20Stock-ROI&cm_mmc=AdieSEM_PLA_Google-_-GS-UK-Categories-Branded-High%20Stock-ROI-_-Branded%20High%20Stock%20-%20unisex-_-PRODUCT_GROUP&cm_mmca1=UK&cm_mmca2=&ds_agid=58700007370013582&gad_source=1&gclid=EAIaIQobChMIyqj8qP3DggMVR1ZyCh1C5wGBEAQYASABEgIiofD_BwE&gclsrc=aw.ds&is_retargeting=true&pid=googleadwords_temp",
            "https://www.clarks.com/en-gb/badell-top/26173418-p?gclid=CjwKCAiA0syqBhBxEiwAeNx9NwR4dG0Y3BwAygtvDqvzT_XWxnOFxvJAP6scRPCbV_JhuT731x4S6RoCxn0QAvD_BwE&gclsrc=aw.ds",
            "https://www.marksandspencer.com/3pk-regular-cotton-blend-long-sleeve-shirts/p/clp60546193#intid=pid_pg1pip48g4r2c3",
            "https://www.levi.com/GB/en_GB/clothing/men/501-original-jeans/p/005013383?camp=SHB_Levis_UK_EN_GOO_Performance_Max&gad=1&gclid=EAIaIQobChMI7ayWrILEggMV2VBHAR2jMAq-EAQYCCABEgKPbPD_BwE&gclsrc=aw.ds",
            "https://www.thenorthface.co.uk/shop/en/tnf-gb/mens-lhotse-down-jacket-3y23?variationId=N14&gclid=EAIaIQobChMIje3awYLEggMVrfPICh0ixQXoEAQYBSABEgINefD_BwE&gclsrc=aw.ds",
            "https://www.timberland.co.uk/shop/en/tbl-uk/new/timberland-premium-6-inch-boot-for-men-in-grey-a62bh033",
            "https://uk.puma.com/uk/en/pd/palermo-lth-unisex-sneakers/396464?swatch=01&size=0290&gad_source=1&gclid=EAIaIQobChMI9MXf8IjEggMVE-3ICh1E_g63EAQYAiABEgKoCfD_BwE",
            "https://www.lacoste.com/gb/lacoste/men/clothing/sweatshirts/paris-jacquard-monogram-zipped-sweatshirt/3617070338383.html?wiz_source=google&wiz_medium=cpc&wiz_campaign=17865838514&gclid=CjwKCAiA0syqBhBxEiwAeNx9N1OY6ASWjS17cXqqt0oeld07kV6XQ0wxHYXgOvaPcPba72MIQpEknhoCCvQQAvD_BwE",
            "https://www.ralphlauren.co.uk/en/custom-fit-plaid-oxford-shirt-650468.html?dwvar650468_colorname=6134%20Red%2FGreen%20Multi&cgid=men-clothing-casual-shirts&webcat=men%2Fclothing%2Fmen%20clothing%20casual%20shirts#webcat=men%7Cclothing%7Cmen-clothing-casual-shirts&start=1&cgid=men-clothing-casual-shirts",
            "https://www.zara.com/uk/en/hoodie-p00761350.html?v1=311297480&v2=2299536",
            "https://www2.hm.com/en_gb/productpage.1171208002.html",
            "https://www.gap.co.uk/style/ls413782/k69973#k69973",
            "https://www.oakley.com/en-gb/product/FOA404861?variant=193517897154",
            "https://aldoshoes.co.uk/collections/mens-shoes-trainers/products/lonespec-black-13614539",
            "https://uk.tommy.com/1985-collection-regular-fit-shorts-mw0mw23563rbl",
            "https://www.g-star.com/en_gb/shop/men/jeans/51001-c910-c778",
            "https://www.calvinklein.co.uk/3-pack-boxer-briefs-micro-stretch-wicking-000nb2570aub1",
            "https://www.asics.com/gb/en-gb/gel-nimbus-25/p/1011B547-004.html",
            "https://www.uniqlo.com/uk/en/product/flannel-easy-patterned-ankle-length-trousers-460277.html",
        ],
    },
}
# FASHION EXPERIMENT COOKIE BANNER UK VPN
fashion_uk_nothing_cookies = {
    "experiment_name": "fashion_uk_nothing_cookies",
    "fresh_experiment": True,
    "use_custom_pages": True,
    "cookie_banner_action": 0,
    "custom_pages_params": {
        "categorize_pages": False,  # If True we load the TrainingPagesHandler with the custom_pages_list=custom_pages_list, under custom_list=True, list_id={experiment_name}.
        "custom_pages_list": [  # List of URLs provided by the user. This list is not saved in the {experiment_name}_config.json, since it has to first be categorized and then accessed through the TrainingPagesHandler().get_training_pages_by_category() method.
            "https://www.canterbury.com/mens-ireland-team-polo-shirt-blue/13976088.html",
            "https://www.adidas.co.uk/samba-og-shoes/B75806.html?af_channel=Shopping_Search&af_reengagement_window=30d&c=GS-UK-Categories-Branded-High%20Stock-ROI&cm_mmc=AdieSEM_PLA_Google-_-GS-UK-Categories-Branded-High%20Stock-ROI-_-Branded%20High%20Stock%20-%20unisex-_-PRODUCT_GROUP&cm_mmca1=UK&cm_mmca2=&ds_agid=58700007370013582&gad_source=1&gclid=EAIaIQobChMIyqj8qP3DggMVR1ZyCh1C5wGBEAQYASABEgIiofD_BwE&gclsrc=aw.ds&is_retargeting=true&pid=googleadwords_temp",
            "https://www.clarks.com/en-gb/badell-top/26173418-p?gclid=CjwKCAiA0syqBhBxEiwAeNx9NwR4dG0Y3BwAygtvDqvzT_XWxnOFxvJAP6scRPCbV_JhuT731x4S6RoCxn0QAvD_BwE&gclsrc=aw.ds",
            "https://www.marksandspencer.com/3pk-regular-cotton-blend-long-sleeve-shirts/p/clp60546193#intid=pid_pg1pip48g4r2c3",
            "https://www.levi.com/GB/en_GB/clothing/men/501-original-jeans/p/005013383?camp=SHB_Levis_UK_EN_GOO_Performance_Max&gad=1&gclid=EAIaIQobChMI7ayWrILEggMV2VBHAR2jMAq-EAQYCCABEgKPbPD_BwE&gclsrc=aw.ds",
            "https://www.thenorthface.co.uk/shop/en/tnf-gb/mens-lhotse-down-jacket-3y23?variationId=N14&gclid=EAIaIQobChMIje3awYLEggMVrfPICh0ixQXoEAQYBSABEgINefD_BwE&gclsrc=aw.ds",
            "https://www.timberland.co.uk/shop/en/tbl-uk/new/timberland-premium-6-inch-boot-for-men-in-grey-a62bh033",
            "https://uk.puma.com/uk/en/pd/palermo-lth-unisex-sneakers/396464?swatch=01&size=0290&gad_source=1&gclid=EAIaIQobChMI9MXf8IjEggMVE-3ICh1E_g63EAQYAiABEgKoCfD_BwE",
            "https://www.lacoste.com/gb/lacoste/men/clothing/sweatshirts/paris-jacquard-monogram-zipped-sweatshirt/3617070338383.html?wiz_source=google&wiz_medium=cpc&wiz_campaign=17865838514&gclid=CjwKCAiA0syqBhBxEiwAeNx9N1OY6ASWjS17cXqqt0oeld07kV6XQ0wxHYXgOvaPcPba72MIQpEknhoCCvQQAvD_BwE",
            "https://www.ralphlauren.co.uk/en/custom-fit-plaid-oxford-shirt-650468.html?dwvar650468_colorname=6134%20Red%2FGreen%20Multi&cgid=men-clothing-casual-shirts&webcat=men%2Fclothing%2Fmen%20clothing%20casual%20shirts#webcat=men%7Cclothing%7Cmen-clothing-casual-shirts&start=1&cgid=men-clothing-casual-shirts",
            "https://www.zara.com/uk/en/hoodie-p00761350.html?v1=311297480&v2=2299536",
            "https://www2.hm.com/en_gb/productpage.1171208002.html",
            "https://www.gap.co.uk/style/ls413782/k69973#k69973",
            "https://www.oakley.com/en-gb/product/FOA404861?variant=193517897154",
            "https://aldoshoes.co.uk/collections/mens-shoes-trainers/products/lonespec-black-13614539",
            "https://uk.tommy.com/1985-collection-regular-fit-shorts-mw0mw23563rbl",
            "https://www.g-star.com/en_gb/shop/men/jeans/51001-c910-c778",
            "https://www.calvinklein.co.uk/3-pack-boxer-briefs-micro-stretch-wicking-000nb2570aub1",
            "https://www.asics.com/gb/en-gb/gel-nimbus-25/p/1011B547-004.html",
            "https://www.uniqlo.com/uk/en/product/flannel-easy-patterned-ankle-length-trousers-460277.html",
        ],
    },
}


# HOW TO RUN:
# from oba_crawler import OBAMeasurementExperiment
# experiment_handler = OBAMeasurementExperiment(**fashion_uk_accept_cookies)
# experiment_handler.start(1)
