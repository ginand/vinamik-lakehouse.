-- ============================================================
-- VinaMilk Data Lakehouse — Master Data Initialization
-- Realistic Vietnamese business data for VinaMilk Corporation
-- ============================================================

-- ─────────────────────────────────────────────────────────
-- COMPANY CODES — VinaMilk Group entities
-- ─────────────────────────────────────────────────────────
INSERT INTO company_codes (bukrs, company_name, country, currency, tax_code, address) VALUES
('1000', 'Công ty Cổ phần Sữa Việt Nam (VinaMilk)', 'VN', 'VND', '0300588569', '10 Tân Trào, Tân Phú, Q.7, TP.HCM'),
('1100', 'Công ty TNHH Sữa Lâm Đồng (Dalat Milk - Subsidiary)', 'VN', 'VND', '5800231461', '04 Phù Đổng Thiên Vương, TP. Đà Lạt'),
('1200', 'Công ty CP Sữa Mộc Châu (VinaMilk-Mocchau)', 'VN', 'VND', '2700290013', 'Khu công nghiệp Mộc Châu, Sơn La'),
('2000', 'VinaMilk International Singapore Pte. Ltd', 'SG', 'SGD', 'SG201512345A', '1 Raffles Place, Singapore');

-- ─────────────────────────────────────────────────────────
-- PLANTS — VinaMilk Factories & Distribution Centers
-- ─────────────────────────────────────────────────────────
INSERT INTO plants (plant_id, plant_name, bukrs, city, province, region, plant_type, capacity_tons_day) VALUES
-- Factories (South)
('VM01', 'Nhà máy sữa Thống Nhất', '1000', 'Quận 12', 'TP. Hồ Chí Minh', 'SOUTH', 'FACTORY', 400),
('VM02', 'Nhà máy sữa Trường Thọ', '1000', 'Thủ Đức', 'TP. Hồ Chí Minh', 'SOUTH', 'FACTORY', 500),
('VM03', 'Nhà máy sữa Dielac', '1000', 'Biên Hòa', 'Đồng Nai', 'SOUTH', 'FACTORY', 350),
-- Factories (Central)
('VM04', 'Nhà máy sữa Nghệ An', '1000', 'Vinh', 'Nghệ An', 'CENTRAL', 'FACTORY', 400),
('VM05', 'Nhà máy sữa Đà Nẵng', '1000', 'Đà Nẵng', 'Đà Nẵng', 'CENTRAL', 'FACTORY', 200),
-- Factories (North)
('VM06', 'Nhà máy sữa Hà Nội', '1000', 'Gia Lâm', 'Hà Nội', 'NORTH', 'FACTORY', 300),
('VM07', 'Nhà máy sữa Cần Thơ', '1000', 'Cần Thơ', 'Cần Thơ', 'SOUTH', 'FACTORY', 200),
-- Dairy Farms
('VF01', 'Trang trại bò sữa Tây Ninh', '1000', 'Tây Ninh', 'Tây Ninh', 'SOUTH', 'WAREHOUSE', NULL),
('VF02', 'Trang trại bò sữa Lâm Đồng', '1000', 'Đà Lạt', 'Lâm Đồng', 'CENTRAL', 'WAREHOUSE', NULL),
-- Subsidiary plants
('MC01', 'Nhà máy Sữa Mộc Châu (subsidiary)', '1200', 'Mộc Châu', 'Sơn La', 'NORTH', 'FACTORY', 300);

-- ─────────────────────────────────────────────────────────
-- COST CENTERS — By department and factory
-- Format: [DEPT][PLANT] e.g. PRD-VM01 = Production Dept, Thống Nhất factory
-- ─────────────────────────────────────────────────────────
INSERT INTO cost_centers (cost_center_id, cost_center_name, plant_id, bukrs, cc_type, responsible) VALUES
-- Production departments (by factory)
('PRD-VM01', 'Sản xuất - Nhà máy Thống Nhất', 'VM01', '1000', 'PRODUCTION', 'Nguyễn Văn Minh'),
('PRD-VM02', 'Sản xuất - Nhà máy Trường Thọ', 'VM02', '1000', 'PRODUCTION', 'Trần Thị Hoa'),
('PRD-VM03', 'Sản xuất - Nhà máy Dielac', 'VM03', '1000', 'PRODUCTION', 'Lê Quốc Hùng'),
('PRD-VM04', 'Sản xuất - Nhà máy Nghệ An', 'VM04', '1000', 'PRODUCTION', 'Phạm Văn An'),
('PRD-VM06', 'Sản xuất - Nhà máy Hà Nội', 'VM06', '1000', 'PRODUCTION', 'Hoàng Minh Tuấn'),
-- Sales departments (by channel)
('SAL-MT-S', 'Kinh doanh MT - Miền Nam', 'VM02', '1000', 'SALES', 'Nguyễn Thị Lan'),
('SAL-MT-N', 'Kinh doanh MT - Miền Bắc', 'VM06', '1000', 'SALES', 'Đinh Quang Hà'),
('SAL-TT-S', 'Kinh doanh TT - Miền Nam', 'VM01', '1000', 'SALES', 'Võ Thành Phát'),
('SAL-TT-N', 'Kinh doanh TT - Miền Bắc', 'VM06', '1000', 'SALES', 'Bùi Thị Nga'),
('SAL-EXP',  'Xuất khẩu Quốc tế', 'VM02', '1000', 'SALES', 'Lý Thị Thanh'),
-- Admin & management
('ADM-HCM',  'Hành chính - Trụ sở HCM', 'VM02', '1000', 'ADMIN', 'Trương Thị Bích'),
('ADM-HAN',  'Hành chính - Văn phòng HN', 'VM06', '1000', 'ADMIN', 'Đỗ Văn Long'),
-- Logistics
('LOG-DC-S', 'Kho phân phối - Miền Nam', 'VM01', '1000', 'LOGISTICS', 'Phan Văn Đức'),
('LOG-DC-N', 'Kho phân phối - Miền Bắc', 'VM06', '1000', 'LOGISTICS', 'Ngô Thị Phương'),
-- R&D
('RND-001',  'Nghiên cứu & Phát triển sản phẩm', 'VM03', '1000', 'RND', 'Lê Thị Xuân'),
-- Subsidiary
('PRD-MC01', 'Sản xuất - Mộc Châu Milk', 'MC01', '1200', 'PRODUCTION', 'Cầm Văn Thành');

-- ─────────────────────────────────────────────────────────
-- CHART OF ACCOUNTS — VAS (Thông tư 200/2014/TT-BTC)
-- Core accounts used in VinaMilk's daily operations
-- ─────────────────────────────────────────────────────────
INSERT INTO chart_of_accounts (account_id, account_name, account_type, account_group, normal_balance, allows_cost_center, requires_partner, is_reconciliation) VALUES
-- ===== TÀI SẢN (ASSETS) 1xx =====
('111',   'Tiền mặt', 'ASSET', 'CASH', 'D', FALSE, FALSE, FALSE),
('1121',  'Tiền gửi VCB - VND', 'ASSET', 'BANK', 'D', FALSE, FALSE, FALSE),
('1122',  'Tiền gửi Techcombank - VND', 'ASSET', 'BANK', 'D', FALSE, FALSE, FALSE),
('1123',  'Tiền gửi BIDV - VND', 'ASSET', 'BANK', 'D', FALSE, FALSE, FALSE),
('1124',  'Tiền gửi VCB - USD', 'ASSET', 'BANK', 'D', FALSE, FALSE, FALSE),
('1125',  'Tiền gửi VCB - EUR', 'ASSET', 'BANK', 'D', FALSE, FALSE, FALSE),
('131',   'Phải thu khách hàng', 'ASSET', 'RECEIVABLE', 'D', FALSE, TRUE, TRUE),
('1331',  'Thuế GTGT đầu vào được khấu trừ', 'ASSET', 'TAX', 'D', FALSE, FALSE, FALSE),
('141',   'Tạm ứng nhân viên', 'ASSET', 'OTHER', 'D', TRUE, FALSE, FALSE),
('152',   'Nguyên vật liệu (sữa tươi, đường, bao bì)', 'ASSET', 'INVENTORY', 'D', TRUE, FALSE, FALSE),
('154',   'Chi phí SXKD dở dang', 'ASSET', 'INVENTORY', 'D', TRUE, FALSE, FALSE),
('155',   'Thành phẩm (sữa, kem, yogurt)', 'ASSET', 'INVENTORY', 'D', TRUE, FALSE, FALSE),
('156',   'Hàng hóa tại kho phân phối', 'ASSET', 'INVENTORY', 'D', TRUE, FALSE, FALSE),
('211',   'Nhà xưởng và thiết bị sản xuất', 'ASSET', 'FIXED_ASSET', 'D', FALSE, FALSE, FALSE),
('213',   'Phương tiện vận tải, truyền dẫn', 'ASSET', 'FIXED_ASSET', 'D', FALSE, FALSE, FALSE),
('214',   'Hao mòn TSCĐ lũy kế', 'ASSET', 'FIXED_ASSET', 'C', FALSE, FALSE, FALSE),
('228',   'Đầu tư tài chính dài hạn khác', 'ASSET', 'INVESTMENT', 'D', FALSE, FALSE, FALSE),
-- ===== NỢ PHẢI TRẢ (LIABILITIES) 3xx =====
('331',   'Phải trả người bán (NCC)', 'LIABILITY', 'PAYABLE', 'C', FALSE, TRUE, TRUE),
('333',   'Thuế và các khoản phải nộp nhà nước', 'LIABILITY', 'TAX', 'C', FALSE, FALSE, FALSE),
('3331',  'Thuế GTGT phải nộp (VAT)', 'LIABILITY', 'TAX', 'C', FALSE, FALSE, FALSE),
('3334',  'Thuế thu nhập doanh nghiệp (TNDN)', 'LIABILITY', 'TAX', 'C', FALSE, FALSE, FALSE),
('334',   'Phải trả người lao động (lương)', 'LIABILITY', 'PAYROLL', 'C', FALSE, FALSE, FALSE),
('3383',  'Bảo hiểm xã hội (phần chủ DN đóng)', 'LIABILITY', 'PAYROLL', 'C', FALSE, FALSE, FALSE),
('3384',  'Bảo hiểm y tế (phần chủ DN đóng)', 'LIABILITY', 'PAYROLL', 'C', FALSE, FALSE, FALSE),
('341',   'Vay ngân hàng dài hạn', 'LIABILITY', 'LOAN', 'C', FALSE, FALSE, FALSE),
('3411',  'Vay ngắn hạn VCB', 'LIABILITY', 'LOAN', 'C', FALSE, FALSE, FALSE),
('3412',  'Vay ngắn hạn Techcombank', 'LIABILITY', 'LOAN', 'C', FALSE, FALSE, FALSE),
-- ===== VỐN CHỦ SỞ HỮU (EQUITY) 4xx =====
('411',   'Vốn đầu tư của chủ sở hữu', 'EQUITY', 'EQUITY', 'C', FALSE, FALSE, FALSE),
('414',   'Quỹ đầu tư phát triển', 'EQUITY', 'EQUITY', 'C', FALSE, FALSE, FALSE),
('421',   'Lợi nhuận chưa phân phối', 'EQUITY', 'EQUITY', 'C', FALSE, FALSE, FALSE),
-- ===== DOANH THU (REVENUE) 5xx =====
('511',   'Doanh thu bán hàng nội địa', 'REVENUE', 'REVENUE', 'C', TRUE, FALSE, FALSE),
('5111',  'Doanh thu - Sữa tươi UHT (nội địa)', 'REVENUE', 'REVENUE', 'C', TRUE, FALSE, FALSE),
('5112',  'Doanh thu - Sữa đặc Ông Thọ', 'REVENUE', 'REVENUE', 'C', TRUE, FALSE, FALSE),
('5113',  'Doanh thu - Sữa bột Dielac', 'REVENUE', 'REVENUE', 'C', TRUE, FALSE, FALSE),
('5114',  'Doanh thu - Sữa chua Vinamilk', 'REVENUE', 'REVENUE', 'C', TRUE, FALSE, FALSE),
('5115',  'Doanh thu - Kem & Nước giải khát', 'REVENUE', 'REVENUE', 'C', TRUE, FALSE, FALSE),
('5121',  'Doanh thu xuất khẩu (ngoại tệ)', 'REVENUE', 'REVENUE', 'C', TRUE, FALSE, FALSE),
('515',   'Doanh thu hoạt động tài chính (lãi gửi)', 'REVENUE', 'FINANCE', 'C', FALSE, FALSE, FALSE),
('521',   'Chiết khấu thương mại', 'REVENUE', 'REVENUE', 'D', FALSE, FALSE, FALSE),
('5211',  'Chiết khấu MT - thương lượng', 'REVENUE', 'REVENUE', 'D', FALSE, FALSE, FALSE),
-- ===== CHI PHÍ (EXPENSES) 6xx =====
('621',   'Chi phí NVL trực tiếp (sữa tươi, đường)', 'EXPENSE', 'COGS', 'D', TRUE, FALSE, FALSE),
('622',   'Chi phí nhân công trực tiếp SX', 'EXPENSE', 'COGS', 'D', TRUE, FALSE, FALSE),
('627',   'Chi phí sản xuất chung (khấu hao, điện)', 'EXPENSE', 'COGS', 'D', TRUE, FALSE, FALSE),
('6271',  'Khấu hao TSCĐ nhà máy', 'EXPENSE', 'COGS', 'D', TRUE, FALSE, FALSE),
('6272',  'Chi phí điện nước nhà máy', 'EXPENSE', 'COGS', 'D', TRUE, FALSE, FALSE),
('632',   'Giá vốn hàng bán', 'EXPENSE', 'COGS', 'D', TRUE, FALSE, FALSE),
('635',   'Chi phí tài chính (lãi vay)', 'EXPENSE', 'FINANCE', 'D', FALSE, FALSE, FALSE),
('641',   'Chi phí bán hàng', 'EXPENSE', 'OPEX', 'D', TRUE, FALSE, FALSE),
('6411',  'Chi phí nhân viên bán hàng (lương)', 'EXPENSE', 'OPEX', 'D', TRUE, FALSE, FALSE),
('6412',  'Chi phí quảng cáo, tiếp thị', 'EXPENSE', 'OPEX', 'D', TRUE, FALSE, FALSE),
('6413',  'Chi phí vận chuyển, phân phối', 'EXPENSE', 'OPEX', 'D', TRUE, FALSE, FALSE),
('642',   'Chi phí quản lý doanh nghiệp (QLDN)', 'EXPENSE', 'OPEX', 'D', TRUE, FALSE, FALSE),
('6421',  'Lương nhân viên quản lý', 'EXPENSE', 'OPEX', 'D', TRUE, FALSE, FALSE),
('6422',  'Chi phí văn phòng, thuê mặt bằng', 'EXPENSE', 'OPEX', 'D', TRUE, FALSE, FALSE),
('6423',  'Chi phí khấu hao văn phòng', 'EXPENSE', 'OPEX', 'D', TRUE, FALSE, FALSE),
('711',   'Thu nhập khác (thanh lý TS)', 'REVENUE', 'OTHER', 'C', FALSE, FALSE, FALSE),
('811',   'Chi phí khác', 'EXPENSE', 'OTHER', 'D', FALSE, FALSE, FALSE);

-- ─────────────────────────────────────────────────────────
-- CUSTOMERS — VinaMilk's distribution partners
-- Types: MT=Modern Trade, TT=Traditional Trade, EXPORT, GT=General Trade
-- ─────────────────────────────────────────────────────────
INSERT INTO customers (customer_id, customer_name, customer_type, tax_code, phone, email, city, province, country, sales_region, sales_channel, credit_limit, payment_terms, currency) VALUES
-- === MODERN TRADE (MT) - Siêu thị ===
('CUST-MT001', 'Công ty TNHH Big C Việt Nam (BigC)', 'MT', '0102425982', '028-38128011', 'ap@bigc.com.vn', 'Quận 10', 'TP. Hồ Chí Minh', 'VN', 'SOUTH', 'MODERN_TRADE', 5000000000, 'NET30', 'VND'),
('CUST-MT002', 'Công ty TNHH Lotte Mart Việt Nam', 'MT', '0312345678', '028-35261006', 'purchase@lottemart.com.vn', 'Quận 7', 'TP. Hồ Chí Minh', 'VN', 'SOUTH', 'MODERN_TRADE', 4000000000, 'NET30', 'VND'),
('CUST-MT003', 'Liên hiệp HTX Thương mại TP.HCM (Saigon Co.op)', 'MT', '0300588569', '028-38354161', 'purchase@saigonco.op', 'Quận 3', 'TP. Hồ Chí Minh', 'VN', 'SOUTH', 'MODERN_TRADE', 8000000000, 'NET30', 'VND'),
('CUST-MT004', 'Công ty CP Vincommerce (VinMart/Winmart)', 'MT', '0102671359', '024-39244566', 'purchase@vincommerce.com.vn', 'Đống Đa', 'Hà Nội', 'VN', 'NORTH', 'MODERN_TRADE', 6000000000, 'NET15', 'VND'),
('CUST-MT005', 'Công ty TNHH MM Mega Market Việt Nam', 'MT', '0301388866', '028-38127000', 'purchase@mmvietnam.com', 'Bình Dương', 'Bình Dương', 'VN', 'SOUTH', 'MODERN_TRADE', 3500000000, 'NET30', 'VND'),
('CUST-MT006', 'Công ty CP Thế Giới Di Động (BHX - Bách Hóa Xanh)', 'MT', '0303372806', '028-38105000', 'b2b@bachhoaxanh.com', 'Quận 1', 'TP. Hồ Chí Minh', 'VN', 'SOUTH', 'MODERN_TRADE', 3000000000, 'NET15', 'VND'),
('CUST-MT007', 'Công ty TNHH Central Retail Việt Nam (GO!/Big C)', 'MT', '0101384620', '028-38226666', 'dairy@centralretail.com.vn', 'Quận 1', 'TP. Hồ Chí Minh', 'VN', 'SOUTH', 'MODERN_TRADE', 4500000000, 'NET30', 'VND'),
('CUST-MT008', 'Hệ thống siêu thị Hapro (Hà Nội)', 'MT', '0101124968', '024-38513461', 'purchase@hapro.com.vn', 'Hai Bà Trưng', 'Hà Nội', 'VN', 'NORTH', 'MODERN_TRADE', 2000000000, 'NET30', 'VND'),

-- === TRADITIONAL TRADE (TT) - Nhà phân phối ===
('CUST-TT001', 'Công ty CP Phân Phối Sữa Miền Nam', 'TT', '0310456789', '028-39123456', 'order@ppsmn.com.vn', 'Quận 8', 'TP. Hồ Chí Minh', 'VN', 'SOUTH', 'TRADITIONAL', 2000000000, 'NET30', 'VND'),
('CUST-TT002', 'Đại lý Sữa Vinamilk Hà Nội (NDP-HN01)', 'TT', '0105987654', '024-66562345', 'ndp.hn01@vinamilk-dp.com', 'Hoàn Kiếm', 'Hà Nội', 'VN', 'NORTH', 'TRADITIONAL', 1500000000, 'NET30', 'VND'),
('CUST-TT003', 'Đại lý phân phối Đà Nẵng (NDP-DN01)', 'TT', '0401234567', '0236-3987654', 'ndp.dn01@vinamilk-dp.com', 'Hải Châu', 'Đà Nẵng', 'VN', 'CENTRAL', 'TRADITIONAL', 1200000000, 'NET30', 'VND'),
('CUST-TT004', 'Đại lý phân phối Cần Thơ (NDP-CT01)', 'TT', '1800453210', '0292-3876543', 'ndp.ct01@vinamilk-dp.com', 'Ninh Kiều', 'Cần Thơ', 'VN', 'SOUTH', 'TRADITIONAL', 1000000000, 'NET30', 'VND'),
('CUST-TT005', 'Đại lý phân phối Nghệ An (NDP-NA01)', 'TT', '2901678901', '0238-3654321', 'ndp.na01@vinamilk-dp.com', 'Vinh', 'Nghệ An', 'VN', 'CENTRAL', 'TRADITIONAL', 800000000, 'NET45', 'VND'),
('CUST-TT006', 'Đại lý phân phối Bình Dương (NDP-BD01)', 'TT', '3702987654', '0274-3789654', 'ndp.bd01@vinamilk-dp.com', 'Thủ Dầu Một', 'Bình Dương', 'VN', 'SOUTH', 'TRADITIONAL', 1300000000, 'NET30', 'VND'),
('CUST-TT007', 'Đại lý phân phối Hải Phòng (NDP-HP01)', 'TT', '0200876543', '0225-3456789', 'ndp.hp01@vinamilk-dp.com', 'Hồng Bàng', 'Hải Phòng', 'VN', 'NORTH', 'TRADITIONAL', 900000000, 'NET30', 'VND'),
('CUST-TT008', 'Đại lý phân phối Long An (NDP-LA01)', 'TT', '1101765432', '0272-3543210', 'ndp.la01@vinamilk-dp.com', 'Tân An', 'Long An', 'VN', 'SOUTH', 'TRADITIONAL', 700000000, 'NET45', 'VND'),
('CUST-TT009', 'Đại lý phân phối Đắk Lắk (NDP-DL01)', 'TT', '6001654321', '0262-3210987', 'ndp.dl01@vinamilk-dp.com', 'Buôn Ma Thuột', 'Đắk Lắk', 'VN', 'CENTRAL', 'TRADITIONAL', 600000000, 'NET45', 'VND'),
('CUST-TT010', 'Đại lý phân phối Tiền Giang (NDP-TG01)', 'TT', '8201543210', '0273-3432109', 'ndp.tg01@vinamilk-dp.com', 'Mỹ Tho', 'Tiền Giang', 'VN', 'SOUTH', 'TRADITIONAL', 650000000, 'NET45', 'VND'),

-- === EXPORT CUSTOMERS ===
('CUST-EX001', 'Almarai Company (Saudi Arabia - Middle East)', 'EXPORT', NULL, '+966-11-4796000', 'dairy@almarai.com', 'Riyadh', 'Riyadh', 'SA', 'EXPORT', 'EXPORT', 15000000000, 'NET60', 'USD'),
('CUST-EX002', 'Fonterra (New Zealand - Import agent)', 'EXPORT', NULL, '+64-9-3742200', 'asia@fonterra.com', 'Auckland', 'Auckland', 'NZ', 'EXPORT', 'EXPORT', 20000000000, 'NET60', 'USD'),
('CUST-EX003', 'Fraser & Neave (F&N) - Singapore', 'EXPORT', NULL, '+65-6318-9393', 'procurement@fn.com.sg', 'Singapore', 'Singapore', 'SG', 'EXPORT', 'EXPORT', 10000000000, 'NET45', 'SGD'),
('CUST-EX004', 'Monde Nissin Philippines (Philippines)', 'EXPORT', NULL, '+63-2-8911-5888', 'dairy@mondenissin.com.ph', 'Manila', 'Metro Manila', 'PH', 'EXPORT', 'EXPORT', 8000000000, 'NET60', 'USD'),
('CUST-EX005', 'Beingmate Group (China)', 'EXPORT', NULL, '+86-571-88906666', 'import@beingmate.com', 'Hangzhou', 'Zhejiang', 'CN', 'EXPORT', 'EXPORT', 12000000000, 'NET60', 'USD'),
('CUST-EX006', 'Angkor Dairy Products (Cambodia - JV)', 'EXPORT', NULL, '+855-23-430430', 'ops@angkordairy.com.kh', 'Phnom Penh', 'Phnom Penh', 'KH', 'EXPORT', 'EXPORT', 5000000000, 'NET30', 'USD'),

-- === GT / HORECA CUSTOMERS ===
('CUST-GT001', 'Công ty CP Tập đoàn Trung Nguyên (Cà phê G7/Trung Nguyên)', 'GT', '0304234567', '028-38360011', 'purchase@trungnguyen.com.vn', 'Quận 3', 'TP. Hồ Chí Minh', 'VN', 'SOUTH', 'GT', 500000000, 'NET30', 'VND'),
('CUST-GT002', 'Công ty TNHH Highlands Coffee Việt Nam', 'GT', '0312987654', '028-38111234', 'supply@highlandscoffee.com.vn', 'Quận 1', 'TP. Hồ Chí Minh', 'VN', 'SOUTH', 'GT', 800000000, 'NET30', 'VND'),
('CUST-GT003', 'Công ty CP The Coffee House', 'GT', '0314765432', '028-39916655', 'supply@thecoffeehouse.com', 'Bình Thạnh', 'TP. Hồ Chí Minh', 'VN', 'SOUTH', 'GT', 600000000, 'NET30', 'VND'),
('CUST-GT004', 'Hệ thống KFC Vietnam (Yum! Restaurants)', 'GT', '0312543210', '028-38225566', 'supply.vn@yum.com', 'Quận 1', 'TP. Hồ Chí Minh', 'VN', 'SOUTH', 'GT', 400000000, 'NET15', 'VND');

-- ─────────────────────────────────────────────────────────
-- VENDORS — VinaMilk's suppliers
-- ─────────────────────────────────────────────────────────
INSERT INTO vendors (vendor_id, vendor_name, vendor_type, tax_code, phone, city, country, bank_name, bank_account, payment_terms, currency) VALUES
-- === RAW MATERIALS - Nguyên vật liệu ===
('VEND-RM001', 'Công ty CP Sữa Mộc Châu (raw milk supply)', 'RAW_MATERIAL', '2700290013', '022-23836589', 'Mộc Châu', 'VN', 'Vietcombank', 'VCB-6789012345', 'NET30', 'VND'),
('VEND-RM002', 'Công ty TNHH Thương mại Toàn Thắng (sữa tươi Ba Vì)', 'RAW_MATERIAL', '0100823456', '024-33861234', 'Hà Nội', 'VN', 'Agribank', 'ARB-8901234567', 'NET30', 'VND'),
('VEND-RM003', 'Công ty CP Đường Quảng Ngãi (đường mía)', 'RAW_MATERIAL', '4300102345', '0255-3823456', 'Quảng Ngãi', 'VN', 'BIDV', 'BID-9012345678', 'NET45', 'VND'),
('VEND-RM004', 'Công ty TNHH Dupont Việt Nam (food additives, vitamins)', 'RAW_MATERIAL', '0312678901', '028-38126677', 'TP. Hồ Chí Minh', 'VN', 'HSBC', 'HSB-0123456789', 'NET30', 'USD'),
('VEND-RM005', 'Kerry Ingredients Việt Nam (hương liệu thực phẩm)', 'RAW_MATERIAL', '0311987654', '028-39878765', 'Bình Dương', 'VN', 'Standard Chartered', 'SCB-1234567890', 'NET45', 'USD'),
('VEND-RM006', 'Công ty CP Việt Trường (whey protein import)', 'RAW_MATERIAL', '0313456789', '028-36543210', 'TP. Hồ Chí Minh', 'VN', 'Techcombank', 'TCB-2345678901', 'NET60', 'USD'),

-- === PACKAGING - Bao bì ===
('VEND-PK001', 'Công ty TNHH Tetra Pak Việt Nam (hộp giấy UHT)', 'PACKAGING', '0301765432', '028-37865432', 'TP. Hồ Chí Minh', 'VN', 'Citibank', 'CTB-3456789012', 'NET30', 'USD'),
('VEND-PK002', 'Công ty CP Nhựa Bình Minh (chai nhựa, hộp)', 'PACKAGING', '0300165432', '028-38275432', 'TP. Hồ Chí Minh', 'VN', 'VPBank', 'VPB-4567890123', 'NET45', 'VND'),
('VEND-PK003', 'Công ty CP Bao bì Đông Á (lon, nhãn nhôm)', 'PACKAGING', '3400234567', '0511-3856543', 'Đà Nẵng', 'VN', 'MB Bank', 'MBB-5678901234', 'NET30', 'VND'),
('VEND-PK004', 'Công ty TNHH Amcor Việt Nam (flexible packaging)', 'PACKAGING', '0310876543', '028-36545678', 'Bình Dương', 'VN', 'ANZ Vietnam', 'ANZ-6789012345', 'NET45', 'USD'),

-- === EQUIPMENT & MAINTENANCE - Thiết bị ===
('VEND-EQ001', 'Công ty TNHH GEA Process Engineering (thiết bị chế biến sữa)', 'EQUIPMENT', '0311234567', '028-39876543', 'TP. Hồ Chí Minh', 'VN', 'Deutsche Bank', 'DEB-7890123456', 'NET60', 'EUR'),
('VEND-EQ002', 'Công ty TNHH Samsung Engineering Việt Nam (bảo trì nhà máy)', 'EQUIPMENT', '0312345678', '028-38765432', 'TP. Hồ Chí Minh', 'VN', 'Woori Bank', 'WRB-8901234567', 'NET30', 'VND'),
('VEND-EQ003', 'Siemens AG Vietnam (hệ thống tự động hóa)', 'EQUIPMENT', '0310123456', '028-38612345', 'TP. Hồ Chí Minh', 'VN', 'Deutsche Bank', 'DEB-9012345678', 'NET60', 'EUR'),

-- === LOGISTICS - Vận chuyển ===
('VEND-LG001', 'Công ty CP Giao Hàng Nhanh (GHN)', 'LOGISTICS', '0312543210', '19001234', 'TP. Hồ Chí Minh', 'VN', 'VPBank', 'VPB-1234567891', 'NET15', 'VND'),
('VEND-LG002', 'Công ty CP Viettel Post', 'LOGISTICS', '0108349829', '1800545464', 'Hà Nội', 'VN', 'Vietcombank', 'VCB-2345678901', 'NET15', 'VND'),
('VEND-LG003', 'Công ty TNHH Kuehne+Nagel Việt Nam (logistics quốc tế)', 'LOGISTICS', '0300985432', '028-38101234', 'TP. Hồ Chí Minh', 'VN', 'HSBC', 'HSB-3456789012', 'NET30', 'USD'),

-- === SERVICES - Dịch vụ ===
('VEND-SV001', 'Công ty Điện lực TP.HCM (EVN-HCM)', 'SERVICE', '0300185389', '028-19001215', 'TP. Hồ Chí Minh', 'VN', 'BIDV', 'BID-4567890123', 'NET15', 'VND'),
('VEND-SV002', 'Công ty CP Cấp nước Gia Định', 'SERVICE', '0300438750', '028-38415456', 'TP. Hồ Chí Minh', 'VN', 'Sacombank', 'SAC-5678901234', 'NET30', 'VND'),
('VEND-SV003', 'Công ty CP Quảng cáo Dentsu One Việt Nam', 'SERVICE', '0312789012', '028-39135678', 'TP. Hồ Chí Minh', 'VN', 'Techcombank', 'TCB-6789012345', 'NET30', 'VND'),
('VEND-SV004', 'PricewaterhouseCoopers Việt Nam (kiểm toán độc lập)', 'SERVICE', '0100100622', '024-39462246', 'Hà Nội', 'VN', 'HSBC', 'HSB-7890123456', 'NET30', 'USD');

-- ─────────────────────────────────────────────────────────
-- Verify master data loaded correctly
-- ─────────────────────────────────────────────────────────
DO $$
DECLARE
    v_company    INTEGER;
    v_plants     INTEGER;
    v_cc         INTEGER;
    v_accounts   INTEGER;
    v_customers  INTEGER;
    v_vendors    INTEGER;
BEGIN
    SELECT COUNT(*) INTO v_company    FROM company_codes;
    SELECT COUNT(*) INTO v_plants     FROM plants;
    SELECT COUNT(*) INTO v_cc         FROM cost_centers;
    SELECT COUNT(*) INTO v_accounts   FROM chart_of_accounts;
    SELECT COUNT(*) INTO v_customers  FROM customers;
    SELECT COUNT(*) INTO v_vendors    FROM vendors;

    RAISE NOTICE '====================================================';
    RAISE NOTICE 'VinaMilk Master Data Loaded:';
    RAISE NOTICE '  Company Codes:   %', v_company;
    RAISE NOTICE '  Plants:          %', v_plants;
    RAISE NOTICE '  Cost Centers:    %', v_cc;
    RAISE NOTICE '  GL Accounts:     % (VAS 200/2014)', v_accounts;
    RAISE NOTICE '  Customers:       % (MT/TT/Export/GT)', v_customers;
    RAISE NOTICE '  Vendors:         % (NVL/Bao bì/DV)', v_vendors;
    RAISE NOTICE 'System ready for Debezium CDC capture!';
    RAISE NOTICE '====================================================';
END $$;
