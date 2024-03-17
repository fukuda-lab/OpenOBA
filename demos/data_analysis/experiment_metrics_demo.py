from oba.experiment_metrics import ExperimentMetrics

analyzer = ExperimentMetrics("style_and_fashion_experiment_accept")
print(analyzer.get_ads_summary())
print(analyzer.get_control_visits_by_url_and_browser())
# print(analyzer.get_visits_by_url_summary())
# print(analyzer.get_landing_pages_summary())
# print(analyzer.get_category_distribution())
analyzer.close()


#     browser_id                            URL  NumVisits
# 0   2902477846              http://adidas.com          6
# 1   2902477846              http://born2be.pl          7
# 2   2902477846            http://cottonon.com          5
# 3   2902477846                 http://dsw.com          5
# 4   2902477846                http://elle.com          6
# 5   2902477846                 http://elle.fr          7
# 6   2902477846         http://fashionnova.com          5
# 7   2902477846                http://guess.eu          8
# 8   2902477846             http://instyle.com          9
# 9   2902477846           http://intersport.fr          4
# 10  2902477846               http://jcrew.com          2
# 11  2902477846                http://kufar.by          5
# 12  2902477846             http://lacoste.com          4
# 13  2902477846                http://levi.com          8
# 14  2902477846         http://michaelkors.com          5
# 15  2902477846                http://temu.com          4
# 16  2902477846         http://therealreal.com          7
# 17  2902477846  http://theweathernetwork.com/          3
# 18  2902477846     http://victoriassecret.com          8
# 19  2902477846               http://vogue.com          3
# 20  2902477846            http://weather.com/          2
# 21  2902477846   http://weather2umbrella.com/          3
# 22  2902477846        http://weatherbase.com/          5
# 23  2902477846                http://wish.com         10
# 24  2902477846                http://yoox.com          5
# 25  2902477846                http://zara.com          6
