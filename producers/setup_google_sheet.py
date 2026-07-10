import os
import random
from datetime import datetime
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from dotenv import load_dotenv
load_dotenv()

# --- HACK: Bypass JWT time validation by fetching real time from Google ---
import time as _time
import datetime as _datetime
import urllib.request as _req
import email.utils as _eutils

_OFFSET = 0
try:
    _r = _req.urlopen(_req.Request("http://google.com", method="HEAD"), timeout=5)
    _real_ts = _eutils.mktime_tz(_eutils.parsedate_tz(_r.headers['Date']))
    _OFFSET = _time.time() - _real_ts
except Exception:
    _OFFSET = 2 * 365 * 24 * 3600

_orig_time = _time.time
def _patched_time():
    return _orig_time() - _OFFSET
_time.time = _patched_time

class PatchedDatetime(_datetime.datetime):
    @classmethod
    def utcnow(cls):
        return super().utcnow() - _datetime.timedelta(seconds=_OFFSET)
    @classmethod
    def now(cls, tz=None):
        return super().now(tz) - _datetime.timedelta(seconds=_OFFSET)
_datetime.datetime = PatchedDatetime
# ----------------------------------------------------------------------

scope = [
    'https://spreadsheets.google.com/feeds',
    'https://www.googleapis.com/auth/drive'
]
CREDENTIALS_FILE = 'producers/credentials.json'

def setup_sheet():
    print("Dang ket noi toi Google Workspace...")
    creds = ServiceAccountCredentials.from_json_keyfile_name(CREDENTIALS_FILE, scope)
    client = gspread.authorize(creds)

    sheet_id = os.getenv("GOOGLE_SHEET_ID", "")
    if not sheet_id:
        print("Loi: Vui long them GOOGLE_SHEET_ID vao file .env truoc khi chay!")
        return
        
    print(f"Dang mo Spreadsheet ID: {sheet_id}...")
    spreadsheet = client.open_by_key(sheet_id)
    sheet_title = spreadsheet.title
    
    # --- Tab 1: Budget_Plan_2026 ---
    try:
        ws_input = spreadsheet.worksheet('Budget_Plan_2026')
        ws_input.clear()
    except gspread.exceptions.WorksheetNotFound:
        ws_input = spreadsheet.get_worksheet(0)
        ws_input.update_title('Budget_Plan_2026')
        ws_input.clear()
    
    print("Xay dung cau truc Enterprise Budget Input...")
    headers = [
        "Budget_ID", "Fiscal_Year", "Month", "Cost_Center", "Department", 
        "Account_Code", "Account_Name", "Product_Group", "Budget_Amount", 
        "Currency", "Approved_By", "Status", "Updated_Date"
    ]
    ws_input.update('A1:M1', [headers])
    
    print("Dang thiet lap format...")
    input_sheet_id = ws_input.id
    
    requests = [
        # Freeze Row 1
        {
            "updateSheetProperties": {
                "properties": {"sheetId": input_sheet_id, "gridProperties": {"frozenRowCount": 1}},
                "fields": "gridProperties.frozenRowCount"
            }
        },
        # Định dạng Header
        {
            "repeatCell": {
                "range": {"sheetId": input_sheet_id, "startRowIndex": 0, "endRowIndex": 1},
                "cell": {
                    "userEnteredFormat": {
                        "backgroundColor": {"red": 0.1, "green": 0.2, "blue": 0.4},
                        "textFormat": {"foregroundColor": {"red": 1, "green": 1, "blue": 1}, "bold": True},
                        "horizontalAlignment": "CENTER"
                    }
                },
                "fields": "userEnteredFormat(backgroundColor,textFormat,horizontalAlignment)"
            }
        },
        # Định dạng số tiền
        {
            "repeatCell": {
                "range": {"sheetId": input_sheet_id, "startRowIndex": 1, "startColumnIndex": 8, "endColumnIndex": 9},
                "cell": {
                    "userEnteredFormat": {
                        "numberFormat": {"type": "NUMBER", "pattern": "#,##0"}
                    }
                },
                "fields": "userEnteredFormat.numberFormat"
            }
        },
        # Xóa các Data Validation (Dropdown) cũ
        {
            "setDataValidation": {
                "range": {"sheetId": input_sheet_id, "startRowIndex": 1, "endRowIndex": 1000, "startColumnIndex": 0, "endColumnIndex": 13}
            }
        }
    ]
    
    spreadsheet.batch_update({"requests": requests})
    
    print("Ghi du lieu mau (tu dong phat sinh 1-12 thang cho core products)...")
    
    sample_data = []
    products = ["Sữa tươi UHT", "Dielac", "Ông Thọ", "Vfresh", "Probi", "Sữa Bột", "Sữa Chua Uống", "Sữa đặc", "Nước ép"]
    departments = [
        ("Sales", "CC-KD01"), ("Sales", "CC-KD02"), ("Sales", "CC-KD03"),
        ("Marketing", "CC-MKT1"), ("Marketing", "CC-MKT2"),
        ("Logistics", "CC-LOG1"), ("Finance", "CC-FIN1")
    ]
    
    # Generate around 500 rows
    row_id = 1
    for i in range(500):
        month = random.randint(1, 12)
        prod = random.choice(products)
        dept, cost_center = random.choice(departments)
        
        budget_id = f"B2026M{month:02d}P{row_id:05d}"
        budget_amt = random.randint(5000, 150000) * 1000000
        account_code = "5111" if "Sữa tươi" in prod else "5113" if "Dielac" in prod else "5112"
        account_name = "Doanh thu bán hàng" if dept == "Sales" else "Chi phí hoạt động"
        
        row = [
            budget_id, 2026, month, cost_center, dept,
            account_code, account_name, prod, budget_amt,
            "VND", "Finance_Director", "APPROVED", datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        ]
        sample_data.append(row)
        row_id += 1
                
    end_row = len(sample_data) + 1
    ws_input.update(f'A2:M{end_row}', sample_data)
    
    print("\nTAO THANH CONG BANG TINH BUDGET PLAN (13 COT)!")
    print(f"Tổng số dòng phát sinh: {len(sample_data)}".encode('utf-8', 'ignore').decode('utf-8'))
    print("=====================================================")
    print(f"Ten Sheet: {sheet_title}".encode('utf-8', 'ignore').decode('utf-8'))
    print(f"Link truy cap: https://docs.google.com/spreadsheets/d/{spreadsheet.id}")
    print("=====================================================")

if __name__ == "__main__":
    setup_sheet()
