import csv
from subprocess import PIPE, Popen
from datetime import datetime
from beancount.ingest import importer
from beancount.core.data import Posting, Transaction, Balance, EMPTY_SET, new_metadata
from beancount.core.amount import Amount
from beancount.core.number import D
from beancount.core.flags import FLAG_OKAY

headers = [
    '區別碼, 銀行碼, 保留, 帳號, 交易日期, 交易金額, 交易摘要, 借貸別(交易金額), 更正記號, 支票號碼, 結餘金額, 備註, 保留, 對方行帳戶/銷帳資料',
    '交易日期,交易時間,支出金額,存入金額,結存餘額,交易摘要,備註,對方帳號,原櫃員機編號,'
    '作帳日,交易日期,交易時間,幣別,支出金額,存入金額,結存餘額,更正,交易摘要,折算匯率,備註,原櫃員機編號,'
]

class Importer(importer.ImporterProtocol):

    def identify(self, file):
        if file.mimetype() != 'text/csv':
            return False
        with open(file.name, 'r') as f:
            text = f.read()
            f.close()
            for h in headers:
                if h in text:
                    return True
            return False

    def file_name(self, file):
        return "bot.csv"

    def file_account(self, file):
        with open(file.name, 'r') as f:
            # account is in the third line
            line1 = f.readline()
            line2 = f.readline()
            if '區別碼, 銀行碼, 保留, 帳號,' in line1:
                acct = line2.split(',')[3].strip()
            else:
                acct = f.readline().split(',')[1].replace("'", "")
            f.close()
            return f'Assets:Bank:BoT'

    def file_date(self, file):
        date = None
        with open(file.name, 'r') as f:
            for row in csv.DictReader(f):
                date = row[' 交易日期'].strip()
            f.close()
        y = date[0:4]
        y = int(y) + 1911
        md = date[4:]
        return datetime.strptime(f'{y}{md}', "%Y%m%d").date()

    def extract(self, file):
        acct = self.file_account(file)
        entries = []
        with open(file.name, 'r') as f:
            for row in csv.DictReader(f):
                dt = row[' 交易日期'].strip()
                y = dt[0:4]
                y = int(y) + 1911
                m = dt[4:6]
                d = dt[6:8]
                date = datetime.strptime(f'{y}/{m}/{d}', "%Y/%m/%d").date()
                payee = row[' 交易摘要'].strip()
                narration = row[' 備註'].strip()
                if ' 借貸別(交易金額)' in row and row[' 借貸別(交易金額)'].strip() == '0':
                    amount = '-' + row[' 交易金額'].strip()
                elif ' 借貸別(交易金額)' in row and row[' 借貸別(交易金額)'].strip() == '1':
                    amount = row[' 交易金額'].strip()
                else:
                    income = row[' 存入金額'].strip('$')
                    expense = row[' 支出金額'].strip('$')
                    amount = income or '-' + expense
                currency = 'TWD'
                amount = Amount(D(amount), currency)

                other_account = 'Expenses:TODO'
                entries.append(Transaction(
                    date=date,
                    payee=payee,
                    narration=narration,
                    meta=new_metadata(file.name, int(1)),
                    flag=FLAG_OKAY,
                    tags=EMPTY_SET,
                    links=EMPTY_SET,
                    postings=[
                        Posting(account=acct, units=amount, cost=None,
                                price=None, flag=None, meta=None),
                        Posting(account=other_account, units=None, cost=None,
                                price=None, flag=None, meta=None)
                    ]
                ))
        return entries
