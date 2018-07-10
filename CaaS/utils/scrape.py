# scrap web pages for article content
# 7/10/18
# updated 7/10/18

import requests
from bs4 import BeautifulSoup


url = 'http://instyle.com/hair/find-perfect-cut-your-face-shape/'
page = requests.get(url)
soup = BeautifulSoup(page.text, 'lxml')
ps = soup.find_all('p')
print('# of p tags found: {}'.format(len(ps)))

for p in ps:
    print(p.prettify())
