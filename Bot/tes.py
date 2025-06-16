import  requests
from bs4 import BeautifulSoup


headers = {
    'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
    'accept-language': 'ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7',
    'cache-control': 'max-age=0',
    'dnt': '1',
    'priority': 'u=0, i',
    'referer': 'https://www.zarplata.ru/search/vacancy/advanced?hhtmFrom=main',
    'sec-ch-ua': '"Google Chrome";v="137", "Chromium";v="137", "Not/A)Brand";v="24"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"Linux"',
    'sec-fetch-dest': 'document',
    'sec-fetch-mode': 'navigate',
    'sec-fetch-site': 'same-origin',
    'sec-fetch-user': '?1',
    'upgrade-insecure-requests': '1',
    'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/137.0.0.0 Safari/537.36',
    # 'cookie': '__ddg9_=94.237.98.68; __ddg1_=CqxkWqIEO2hpNue3bsej; _xsrf=6b1f7446824e7ee17fa9b7c41d60cf46; hhrole=anonymous; display=desktop; crypted_hhuid=DAB56B8F51A10A7D690547B8D1B2324021A79828FAE60FD030044185AD826396; cookies_fixed=true; hhtoken=HQYLlwEj8eDtcZq4XuPaynEpuxib; hhuid=v87WwRCcsIjXXGhOyQ5GVA--; GMT=7; device_magritte_breakpoint=l; tmr_lvid=95c6e3dd893065a22acf3aba86c51cf9; tmr_lvidTS=1749993744863; _ym_uid=1749993745553746234; _ym_d=1749993745; device_breakpoint=l; iap.uid=0f8c10fb210f429d81cda2f09eec7f0c; _ym_isad=2; _ym_visorc=w; domain_sid=OIXRHFYDWUZ3I8jxUe4mI%3A1749993746667; __zzatgib-w-hh=MDA0dBA=Fz2+aQ==; __zzatgib-w-hh=MDA0dBA=Fz2+aQ==; region_clarified=NOT_SET; regions=1204; total_searches=1; tmr_detect=0%7C1749993815834; __ddg8_=3pYBuDzXgVTaFaCt; __ddg10_=1749993836; gsscgib-w-hh=DLGcPzExygOntjPo5mdz4tbznNGBh9awiaQwOem7RGE+lugwWecztPYaGqZOA0vdAMM9ShRa9s50KyLVBX6qqmPb4f1Z1PgCF3I6SmljmfFOg9SwEDTDuVBWWRgPpPY0E7dUDMWGbMegKn+/ShldkkmMc1fZRuAMIc/3DxWvBpQ8KThHEdgHnso7N7dRQh8yZ6z2bbmbZKWOwvVEb3bIPIq8L0hc/SfDCH6Rt/AdR71KpPihTg+1h73XCgpN8D+iZg==; cfidsgib-w-hh=cJ8j63DPdCxfC5YU8QYJBPDyZOuLeZNT/EqFnvK4YNbSJzKB/C7CkxQlgAIpY2lHFjVf604LlRLo4pMGMeMAEe1YL8gBDEp/kmzOJ6Z5tyFeWnTFBWyTpaNlJeEeirY9V8pn78YGmdvXxxga+W+aZBfIfqc1cv7BE/wzIQ==; cfidsgib-w-hh=cJ8j63DPdCxfC5YU8QYJBPDyZOuLeZNT/EqFnvK4YNbSJzKB/C7CkxQlgAIpY2lHFjVf604LlRLo4pMGMeMAEe1YL8gBDEp/kmzOJ6Z5tyFeWnTFBWyTpaNlJeEeirY9V8pn78YGmdvXxxga+W+aZBfIfqc1cv7BE/wzIQ==; cfidsgib-w-hh=cJ8j63DPdCxfC5YU8QYJBPDyZOuLeZNT/EqFnvK4YNbSJzKB/C7CkxQlgAIpY2lHFjVf604LlRLo4pMGMeMAEe1YL8gBDEp/kmzOJ6Z5tyFeWnTFBWyTpaNlJeEeirY9V8pn78YGmdvXxxga+W+aZBfIfqc1cv7BE/wzIQ==; gsscgib-w-hh=DLGcPzExygOntjPo5mdz4tbznNGBh9awiaQwOem7RGE+lugwWecztPYaGqZOA0vdAMM9ShRa9s50KyLVBX6qqmPb4f1Z1PgCF3I6SmljmfFOg9SwEDTDuVBWWRgPpPY0E7dUDMWGbMegKn+/ShldkkmMc1fZRuAMIc/3DxWvBpQ8KThHEdgHnso7N7dRQh8yZ6z2bbmbZKWOwvVEb3bIPIq8L0hc/SfDCH6Rt/AdR71KpPihTg+1h73XCgpN8D+iZg==; gsscgib-w-hh=DLGcPzExygOntjPo5mdz4tbznNGBh9awiaQwOem7RGE+lugwWecztPYaGqZOA0vdAMM9ShRa9s50KyLVBX6qqmPb4f1Z1PgCF3I6SmljmfFOg9SwEDTDuVBWWRgPpPY0E7dUDMWGbMegKn+/ShldkkmMc1fZRuAMIc/3DxWvBpQ8KThHEdgHnso7N7dRQh8yZ6z2bbmbZKWOwvVEb3bIPIq8L0hc/SfDCH6Rt/AdR71KpPihTg+1h73XCgpN8D+iZg==; fgsscgib-w-hh=3d1I417da8bd496a1ecad98a91d3465c6971641c; fgsscgib-w-hh=3d1I417da8bd496a1ecad98a91d3465c6971641c',
}
r = requests.get("https://www.zarplata.ru/search/vacancy?text=&excluded_text=&area=1204&salary=&currency_code=RUR&experience=doesNotMatter&order_by=relevance&search_period=0&items_on_page=50&L_save_area=true&page=0&searchSessionId=5ea21ccb-3be0-4ed9-9ad0-810c3657e6ef", headers=headers)

soup = BeautifulSoup(r.text, "html.parser")

i = soup.find_all('div', class_="magritte-text___pbpft_3-0-41")
for el in i:
    x = el.find('a', class_="magritte-number-pages-action___e3ozw_4-0-53")
    print(x)