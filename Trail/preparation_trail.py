url = 'https://www.le-sportif.com/ResultsDetail/ResultsHistoryDetail_List.aspx?EventResultsID=505&EventResultsActivityID=54640&EventResultsActivityRND=69d27b44-743a-4b94-8c90-91bb8eb93297&SRCHCATEG=SEH&SRCHSEX=M'
response = requests.get(url)
soup = BeautifulSoup(response.text, 'html.parser')

table = soup.find('table')
rows = soup.find_all('tr')

data = []
for row in rows :
    cols = row.find_all('span')
    cols = [ele.text.strip() for ele in cols]
    data.append([ele for ele in cols if ele])

    df = pd.DataFrame(data)

