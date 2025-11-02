from queue import Queue

URLS = [
    [
        "https://ofadaa.com/ibadan/restaurants?term=Buka",
        "https://ofadaa.com/lagos/restaurants?term=Amala+Abula+Canteen",
        "https://ofadaa.com/lagos/restaurants?page=2&term=Amala+Abula+Canteen",
        "https://ofadaa.com/lagos/restaurants?page=3&term=Amala+Abula+Canteen",
        "https://ofadaa.com/lagos/restaurants?page=4&term=Amala+Abula+Canteen",
    ],
    [
        "https://www.eatdrinklagos.com/roll-call/2021/7/5/the-best-amala-spots-in-lagos-part-i",
        "https://www.eatdrinklagos.com/roll-call/discovering-lagos-food-culture-story-of-iya-eba",
    ],
    [
        "https://blog.fusion.ng/2024/09/20/top-5-places-to-try-amala-in-lagos/comment-page-10/",
        "https://blog.fusion.ng/2024/05/20/6-best-places-to-try-amala-in-ibadan/comment-page-2/",
    ],
    [
        "https://foodieinlagos.com/?s=amala",
        "https://foodieinlagos.com/page/2/?s=amala",
        "https://foodieinlagos.com/page/3/?s=amala",
    ],
    [
        "https://foodieinlagos.com/review-ashabi-licking-fingers-food/",
       "https://foodieinlagos.com/review-nok-by-alara/",
        "https://foodieinlagos.com/review-amala-labule-lagos/",
        "https://foodieinlagos.com/mummy-moji-amala-surulere/",
        "https://foodieinlagos.com/review-ajoke-alamala-plus-lagos/",
        "https://foodieinlagos.com/reviewamalashonola/",
        "https://foodieinlagos.com/review-amala-tax-office-surulere/",
        "https://foodieinlagos.com/finding-the-best-amala-spots-in-lagos/",
        "https://foodieinlagos.com/aduke-olowosibi-amala-spot/",
        "https://foodieinlagos.com/abeke-rooftop-restaurant-review-lekki-amala-abula-lagos/",
        "https://foodieinlagos.com/ibile-foods-lagos-amala-review/",
        "https://foodieinlagos.com/yakoyo-2/",
        "https://foodieinlagos.com/review-olasheu-canteen/fy",
        "https://foodieinlagos.com/review-olaoluwa-kitchen/",
        "https://foodieinlagos.com/lets-take-it-local-amala-abula-and-all-the-la-las/",
        "https://foodieinlagos.com/amala-spots-in-lagos/",
        "https://foodieinlagos.com/top-amala-spots-in-lagos/",
        "https://foodieinlagos.com/review-emi-oga/",
        "https://meiza.ng/?s=amala",
        "https://meiza.ng/best-amala-spots-in-lekki-where-to-get-your-swallow-fix-on-the-island/",
    ]
]

sources = Queue()
for url in URLS:
    sources.put(url)

sitemaps = [
    "http://blog.fusion.ng/post-sitemap.xml",
    "http://blog.fusion.ng/page-sitemap.xml",
    "http://blog.fusion.ng/e-landing-page-sitemap.xml",
    "http://blog.fusion.ng/category-sitemap.xml",
    "http://blog.fusion.ng/post_tag-sitemap.xml",
    "http://blog.fusion.ng/post_tag-sitemap2.xml",
    "http://blog.fusion.ng/author-sitemap.xml",

    "https://foodieinlagos.com/post-sitemap.xml",
    "https://foodieinlagos.com/post_tag-sitemap.xml",
    "https://foodieinlagos.com/post_tag-sitemap2.xml",
    "https://foodieinlagos.com/post_tag-sitemap3.xml",
    "https://foodieinlagos.com/post_tag-sitemap4.xml",

    "https://www.eatdrinklagos.com/sitemap.xml",

    "https://ofadaa.s3.amazonaws.com/sitemaps/sitemap.xml.gz",

    "https://meiza.ng/sitemap.xml",
    "https://meiza.ng/sitemap.rss",
]

rss_feeds = [
    "https://meiza.ng/feed/",
    "https://meiza.ng/search/amala/feed/rss2/",

    "https://blog.fusion.ng/feed/",
    "https://blog.fusion.ng/comments/feed/",
]
