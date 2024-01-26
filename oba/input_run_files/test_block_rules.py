import re

from adblockparser import AdblockRules

regex_s = r"(?i)\b(adurl|redirect)=((?:https?:(?:/{1,3}|[a-z0-9%])|[a-z0-9\.\-]+[.](?:com|net|org|edu|gov|mil|aero|asia|biz|cat|coop|info|int|jobs|mobi|museum|name|post|pro|tel|travel|xxx|ac|ad|ae|af|ag|ai|al|am|an|ao|aq|ar|as|at|au|aw|ax|az|ba|bb|bd|be|bf|bg|bh|bi|bj|bm|bn|bo|br|bs|bt|bv|bw|by|bz|ca|cc|cd|cf|cg|ch|ci|ck|cl|cm|cn|co|cr|cs|cu|cv|cx|cy|cz|dd|de|dj|dk|dm|do|dz|ec|ee|eg|eh|er|es|et|eu|fi|fj|fk|fm|fo|fr|ga|gb|gd|ge|gf|gg|gh|gi|gl|gm|gn|gp|gq|gr|gs|gt|gu|gw|gy|hk|hm|hn|hr|ht|hu|id|ie|il|im|in|io|iq|ir|is|it|je|jm|jo|jp|ke|kg|kh|ki|km|kn|kp|kr|kw|ky|kz|la|lb|lc|li|lk|lr|ls|lt|lu|lv|ly|ma|mc|md|me|mg|mh|mk|ml|mm|mn|mo|mp|mq|mr|ms|mt|mu|mv|mw|mx|my|mz|na|nc|ne|nf|ng|ni|nl|no|np|nr|nu|nz|om|pa|pe|pf|pg|ph|pk|pl|pm|pn|pr|ps|pt|pw|py|qa|re|ro|rs|ru|rw|sa|sb|sc|sd|se|sg|sh|si|sj|Ja|sk|sl|sm|sn|so|sr|ss|st|su|sv|sx|sy|sz|tc|td|tf|tg|th|tj|tk|tl|tm|tn|to|tp|tr|tt|tv|tw|tz|ua|ug|uk|us|uy|uz|va|vc|ve|vg|vi|vn|vu|wf|ws|ye|yt|yu|za|zm|zw)/)(?:[^\s()<>{}\[\]]+|\([^\s()]*?\([^\s()]+\)[^\s()]*?\)|\([^\s]+?\))+(?:\([^\s()]*?\([^\s()]+\)[^\s()]*?\)|\([^\s]+?\)|[^\s`!()\[\]{};:\'\".,<>?«»“”‘’])|(?:(?<!@)[a-z0-9]+(?:[.\-][a-z0-9]+)*[.](?:com|net|org|edu|gov|mil|aero|asia|biz|cat|coop|info|int|jobs|mobi|museum|name|post|pro|tel|travel|xxx|ac|ad|ae|af|ag|ai|al|am|an|ao|aq|ar|as|at|au|aw|ax|az|ba|bb|bd|be|bf|bg|bh|bi|bj|bm|bn|bo|br|bs|bt|bv|bw|by|bz|ca|cc|cd|cf|cg|ch|ci|ck|cl|cm|cn|co|cr|cs|cu|cv|cx|cy|cz|dd|de|dj|dk|dm|do|dz|ec|ee|eg|eh|er|es|et|eu|fi|fj|fk|fm|fo|fr|ga|gb|gd|ge|gf|gg|gh|gi|gl|gm|gn|gp|gq|gr|gs|gt|gu|gw|gy|hk|hm|hn|hr|ht|hu|id|ie|il|im|in|io|iq|ir|is|it|je|jm|jo|jp|ke|kg|kh|ki|km|kn|kp|kr|kw|ky|kz|la|lb|lc|li|lk|lr|ls|lt|lu|lv|ly|ma|mc|md|me|mg|mh|mk|ml|mm|mn|mo|mp|mq|mr|ms|mt|mu|mv|mw|mx|my|mz|na|nc|ne|nf|ng|ni|nl|no|np|nr|nu|nz|om|pa|pe|pf|pg|ph|pk|pl|pm|pn|pr|ps|pt|pw|py|qa|re|ro|rs|ru|rw|sa|sb|sc|sd|se|sg|sh|si|sj|Ja|sk|sl|sm|sn|so|sr|ss|st|su|sv|sx|sy|sz|tc|td|tf|tg|th|tj|tk|tl|tm|tn|to|tp|tr|tt|tv|tw|tz|ua|ug|uk|us|uy|uz|va|vc|ve|vg|vi|vn|vu|wf|ws|ye|yt|yu|za|zm|zw)\b/?(?!@)))"

filter_l_l = [
    open(path).read().splitlines() for path in ["easylist.txt", "easyprivacy.txt"]
]
filter_l = [item for sublist in filter_l_l for item in sublist]

ad_block_rules = AdblockRules(filter_l)

href_url = "https://googleads.g.doubleclick.net/pcs/click?xai=AKAOjsuG7ZrdahJsahyerbtwr7-iTaARXq29iSJhFZFSiH0cj1McgMQ2KGhsQtBW-14YPQK4SmAQCF54pK9cbpJIBRDUrxHoLJVNh4urJk_nP8xBphoMzUy-7z4PxdDNqyQYi9wwkfixo_kBo3IifBaXpPI_rz9D296-SdtDIg-xvjlHTwJRwqvmRYyJwTKbKJUIIJIK_is_MuYCfZ_qKhOsy5SvpX6oxMBamd1cj8ok_r6zgiT8f_ZWDhJif1oP2Qm98bGVgeVCOKgbIy8pku7yx9WCeSsnJ4egNJP_nRMdHCcPngIAKL43S5l7fHvVtOc-ehaRJ2TR0GlQs2HcsVLp7RvliSSfnqs1UHrOd5o&sai=AMfl-YSOvRYcrItyAqBHggIkIoFApeuz2XScbxp1SpddjwfpPKG9itEb8hM0srMINgXABBOZ017QGa4aT9smyAirPGVCgFI2zZtiw1t1CE5fRcd_sNYBsH-2thwPew05VVaj1r8FmB7d9l94IGI7TDN14JGbnEuq9A52UC8vnije13sAIQmnP5eyDl0Uq3vOxPXz34pRgZnaR-YelAQyar03U5Pm9w_uBiJ6wCKMiCyq&sig=Cg0ArKJSzDhsQEWbDLe2&fbs_aeid=[gw_fbsaeid]&adurl=http://www.candlewick.com/r/despereaux20-usatoday&nm=4&nx=139&ny=-120&mb=2"

response = ad_block_rules.should_block(href_url)

print(response)
landing_page_url_l = re.findall(regex_s, href_url)

print(landing_page_url_l)
