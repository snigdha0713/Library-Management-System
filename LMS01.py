"""
Library Management System (Python + MySQL)
-----------------------------------------
How to use:
1) pip install mysql-connector-python
2) Update DB_CONFIG with your MySQL credentials
3) Run: python lms.py

"""

import sys
import datetime
import csv

try:
    import mysql.connector as mysql
except ImportError:
    print("mysql-connector-python is not installed. Run: pip install mysql-connector-python")
    sys.exit(1)

# -------------------- CONFIG --------------------
DB_CONFIG = {
    "host": "localhost",
    "user": "root",          # <-- change if needed
    "password": "yourpassword",  # <-- change to your MySQL password
    "database": "librarydb",
}
 
ALLOWED_TABLES = {"books", "staff", "members", "issues", "bills", "bill_items"}

# -------------------- DB --------------------

def get_connection(use_db=True):
    cfg = DB_CONFIG.copy()
    if not use_db:
        cfg.pop("database", None)
    return mysql.connect(**cfg)


def init_database_and_tables():
    con = get_connection(use_db=False)
    cur = con.cursor()
    cur.execute("CREATE DATABASE IF NOT EXISTS librarydb")
    con.commit()
    cur.close()
    con.close()

    con = get_connection(use_db=True)
    cur = con.cursor()

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS books (
            book_id VARCHAR(20) PRIMARY KEY,
            title   VARCHAR(200) NOT NULL,
            author  VARCHAR(100) NOT NULL,
            category VARCHAR(100),
            price   DECIMAL(10,2) NOT NULL,
            stock   INT NOT NULL DEFAULT 0
        ) ENGINE=InnoDB;
        """
    )

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS staff (
            staff_id INT AUTO_INCREMENT PRIMARY KEY,
            name     VARCHAR(100) NOT NULL,
            role     VARCHAR(100),
            phone    VARCHAR(20)
        ) ENGINE=InnoDB;
        """
    )

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS members (
            member_id INT AUTO_INCREMENT PRIMARY KEY,
            name      VARCHAR(100) NOT NULL,
            phone     VARCHAR(20),
            email     VARCHAR(100),
            membership_type VARCHAR(20) NOT NULL DEFAULT 'Regular' -- Regular or VIP
        ) ENGINE=InnoDB;
        """
    )

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS issues (
            issue_id INT AUTO_INCREMENT PRIMARY KEY,
            member_id INT NOT NULL,
            book_id   VARCHAR(20) NOT NULL,
            issue_date DATE NOT NULL,
            due_date   DATE NOT NULL,
            return_date DATE,
            late_fee DECIMAL(8,2) DEFAULT 0.0,
            CONSTRAINT fk_issue_member FOREIGN KEY (member_id) REFERENCES members(member_id),
            CONSTRAINT fk_issue_book   FOREIGN KEY (book_id) REFERENCES books(book_id)
        ) ENGINE=InnoDB;
        """
    )

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS bills (
            bill_id        INT AUTO_INCREMENT PRIMARY KEY,
            member_id      INT,
            bill_date      DATETIME NOT NULL,
            subtotal       DECIMAL(10,2) NOT NULL,
            discount_pct   DECIMAL(5,2)  NOT NULL DEFAULT 0.0,
            discount_amt   DECIMAL(10,2) NOT NULL,
            grand_total    DECIMAL(10,2) NOT NULL,
            CONSTRAINT fk_bill_member FOREIGN KEY (member_id) REFERENCES members(member_id)
        ) ENGINE=InnoDB;
        """
    )

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS bill_items (
            item_id   INT AUTO_INCREMENT PRIMARY KEY,
            bill_id   INT NOT NULL,
            book_id   VARCHAR(20) NOT NULL,
            qty       INT NOT NULL,
            unit_price DECIMAL(10,2) NOT NULL,
            line_total DECIMAL(10,2) NOT NULL,
            CONSTRAINT fk_bill_fk FOREIGN KEY (bill_id) REFERENCES bills(bill_id) ON DELETE CASCADE,
            CONSTRAINT fk_bill_book FOREIGN KEY (book_id) REFERENCES books(book_id)
        ) ENGINE=InnoDB;
        """
    )

    con.commit()
    cur.close()
    con.close()

# -------------------- INPUT --------------------

def input_int(prompt: str, min_val=None, max_val=None):
    while True:
        try:
            val = int(input(prompt))
            if min_val is not None and val < min_val:
                print(f"Value must be >= {min_val}")
                continue
            if max_val is not None and val > max_val:
                print(f"Value must be <= {max_val}")
                continue
            return val
        except ValueError:
            print("Please enter a valid integer.")


def input_float(prompt: str, min_val=None, max_val=None):
    while True:
        try:
            val = float(input(prompt))
            if min_val is not None and val < min_val:
                print(f"Value must be >= {min_val}")
                continue
            if max_val is not None and val > max_val:
                print(f"Value must be <= {max_val}")
                continue
            return val
        except ValueError:
            print("Please enter a valid number.")


def parse_year_month(ym_str: str):
    """Parse 'YYYY-MM' -> (first_date, last_date). Raises ValueError if invalid."""
    parts = ym_str.split("-")
    if len(parts) != 2:
        raise ValueError("Bad format")
    year, month = parts
    year = int(year)
    month = int(month)
    first = datetime.date(year, month, 1)
    if month == 12:
        last = datetime.date(year + 1, 1, 1) - datetime.timedelta(days=1)
    else:
        last = datetime.date(year, month + 1, 1) - datetime.timedelta(days=1)
    return first, last

# -------------------- BOOKS --------------------

def add_book(cur, con):
    print("-- Add Book --")
    book_id = input("Book ID: ").strip()
    title   = input("Title: ").strip()
    author  = input("Author: ").strip()
    category= input("Category (optional): ").strip()
    price   = input_float("Price: ", min_val=0)
    stock   = input_int("Opening Stock: ", min_val=0)

    try:
        cur.execute(
            "INSERT INTO books (book_id, title, author, category, price, stock) VALUES (%s, %s, %s, %s, %s, %s)",
            (book_id, title, author, category, price, stock)
        )
        con.commit()
        print("Book added.")
    except Exception as e:
        msg = str(e)
        if "Duplicate" in msg or "1062" in msg:
            print("Book ID already exists.")
        else:
            print("Error adding book:", msg)


def update_book(cur, con):
    print("-- Update Book --")
    book_id = input("Enter Book ID to update: ").strip()
    cur.execute("SELECT book_id, title, author, category, price, stock FROM books WHERE book_id=%s", (book_id,))
    row = cur.fetchone()
    if not row:
        print("Book not found.")
        return

    print("Leave blank to keep existing value.")
    new_title  = input(f"Title [{row[1]}]: ").strip() or row[1]
    new_author = input(f"Author [{row[2]}]: ").strip() or row[2]
    new_cat    = input(f"Category [{row[3] or ''}]: ").strip() or row[3]
    try:
        price_in = input(f"Price [{row[4]}]: ").strip()
        new_price = float(price_in) if price_in else float(row[4])
    except ValueError:
        print("Invalid price. Keeping old.")
        new_price = float(row[4])
    try:
        stock_in = input(f"Stock [{row[5]}]: ").strip()
        new_stock = int(stock_in) if stock_in else int(row[5])
    except ValueError:
        print("Invalid stock. Keeping old.")
        new_stock = int(row[5])

    cur.execute(
        "UPDATE books SET title=%s, author=%s, category=%s, price=%s, stock=%s WHERE book_id=%s",
        (new_title, new_author, new_cat, new_price, new_stock, book_id)
    )
    con.commit()
    print("Book updated.")


def delete_book(cur, con):
    print("-- Delete Book --")
    book_id = input("Enter Book ID to delete: ").strip()
    cur.execute("DELETE FROM books WHERE book_id=%s", (book_id,))
    con.commit()
    if cur.rowcount:
        print("Book deleted.")
    else:
        print("Book not found.")


def view_books(cur):
    print("-- All Books --")
    cur.execute("SELECT book_id, title, author, category, price, stock FROM books ORDER BY title")
    rows = cur.fetchall()
    if not rows:
        print("(no books)")
        return

    print(f"{'ID':<10} {'Title':<30} {'Author':<20} {'Cat':<12} {'Price':>8} {'Stock':>6}")
    print("-" * 90)
    for r in rows:
        print(f"{r[0]:<10} {r[1]:<30.30} {r[2]:<20.20} { (r[3] or '')[:12]:<12} {r[4]:8} {r[5]:6}")


def search_books(cur):
    print("-- Search Books --")
    print("Search by: 1) Book ID  2) Title  3) Author  4) Category")
    ch = input("Choice: ").strip()
    if ch == '1':
        key = input("Enter Book ID: ").strip()
        cur.execute("SELECT book_id, title, author, category, price, stock FROM books WHERE book_id=%s", (key,))
    elif ch == '2':
        key = '%' + input("Enter keyword for title: ").strip() + '%'
        cur.execute("SELECT book_id, title, author, category, price, stock FROM books WHERE title LIKE %s", (key,))
    elif ch == '3':
        key = '%' + input("Enter author name: ").strip() + '%'
        cur.execute("SELECT book_id, title, author, category, price, stock FROM books WHERE author LIKE %s", (key,))
    elif ch == '4':
        key = '%' + input("Enter category: ").strip() + '%'
        cur.execute("SELECT book_id, title, author, category, price, stock FROM books WHERE category LIKE %s", (key,))
    else:
        print("Invalid choice.")
        return

    rows = cur.fetchall()
    if not rows:
        print("(no results)")
        return
    for r in rows:
        print(f"{r[0]} | {r[1]} | {r[2]} | {r[3] or ''} | Rs.{r[4]} | Stock: {r[5]}")

# -------------------- STAFF --------------------

def add_staff(cur, con):
    print("-- Add Staff --")
    name = input("Name: ").strip()
    role = input("Role: ").strip()
    phone = input("Phone: ").strip()
    cur.execute("INSERT INTO staff (name, role, phone) VALUES (%s, %s, %s)", (name, role, phone))
    con.commit()
    print("Staff added.")


def update_staff(cur, con):
    print("-- Update Staff --")
    staff_id = input_int("Staff ID to update: ", min_val=1)
    cur.execute("SELECT * FROM staff WHERE staff_id=%s", (staff_id,))
    row = cur.fetchone()
    if not row:
        print("Staff not found.")
        return
    print("Leave blank to keep existing value.")
    new_name  = input(f"Name [{row[1]}]: ").strip() or row[1]
    new_role  = input(f"Role [{row[2]}]: ").strip() or row[2]
    new_phone = input(f"Phone [{row[3]}]: ").strip() or row[3]

    cur.execute(
        "UPDATE staff SET name=%s, role=%s, phone=%s WHERE staff_id=%s",
        (new_name, new_role, new_phone, staff_id)
    )
    con.commit()
    print("Staff updated.")


def delete_staff(cur, con):
    print("-- Delete Staff --")
    staff_id = input_int("Staff ID to delete: ", min_val=1)
    cur.execute("DELETE FROM staff WHERE staff_id=%s", (staff_id,))
    con.commit()
    if cur.rowcount:
        print("Staff deleted.")
    else:
        print("Staff not found.")


def view_staff(cur):
    print("-- All Staff --")
    cur.execute("SELECT staff_id, name, role, phone FROM staff ORDER BY staff_id")
    rows = cur.fetchall()
    if not rows:
        print("(no staff)")
        return
    for r in rows:
        print(f"{r[0]} | {r[1]} | {r[2]} | {r[3]}")

# -------------------- MEMBERS & ISSUES --------------------

def add_member(cur, con):
    print("-- Add Member --")
    name = input("Name: ").strip()
    phone = input("Phone: ").strip()
    email = input("Email (optional): ").strip()
    mtype = input("Membership Type (Regular/VIP) [Regular]: ").strip() or 'Regular'
    if mtype not in ('Regular','VIP'):
        mtype = 'Regular'
    cur.execute("INSERT INTO members (name, phone, email, membership_type) VALUES (%s,%s,%s,%s)", (name, phone, email, mtype))
    con.commit()
    print("Member added.")


def update_member(cur, con):
    print("-- Update Member --")
    member_id = input_int("Member ID to update: ", min_val=1)
    cur.execute("SELECT member_id, name, phone, email, membership_type FROM members WHERE member_id=%s", (member_id,))
    row = cur.fetchone()
    if not row:
        print("Member not found.")
        return
    print("Leave blank to keep existing value.")
    new_name = input(f"Name [{row[1]}]: ").strip() or row[1]
    new_phone= input(f"Phone [{row[2]}]: ").strip() or row[2]
    new_email= input(f"Email [{row[3] or ''}]: ").strip() or row[3]
    new_type = input(f"Membership Type [{row[4]}]: ").strip() or row[4]
    if new_type not in ('Regular','VIP'):
        new_type = row[4]
    cur.execute("UPDATE members SET name=%s, phone=%s, email=%s, membership_type=%s WHERE member_id=%s", (new_name, new_phone, new_email, new_type, member_id))
    con.commit()
    print("Member updated.")


def delete_member(cur, con):
    print("-- Delete Member --")
    member_id = input_int("Member ID to delete: ", min_val=1)
    cur.execute("DELETE FROM members WHERE member_id=%s", (member_id,))
    con.commit()
    if cur.rowcount:
        print("Member deleted.")
    else:
        print("Member not found.")


def view_members(cur):
    print("-- Members --")
    cur.execute("SELECT member_id, name, phone, email, membership_type FROM members ORDER BY member_id")
    rows = cur.fetchall()
    if not rows:
        print("(no members)")
        return
    for r in rows:
        print(f"{r[0]} | {r[1]} | {r[2]} | {r[3] or ''} | {r[4]}")


def issue_book(cur, con):
    print("-- Issue Book --")
    member_id = input_int("Member ID: ", min_val=1)
    cur.execute("SELECT name, membership_type FROM members WHERE member_id=%s", (member_id,))
    mrow = cur.fetchone()
    if not mrow:
        print("Member not found.")
        return
    member_name, _ = mrow

    book_id = input("Book ID: ").strip()
    cur.execute("SELECT title, stock FROM books WHERE book_id=%s", (book_id,))
    brow = cur.fetchone()
    if not brow:
        print("Book not found.")
        return
    title, stock = brow
    if stock <= 0:
        print("Book out of stock.")
        return

    days_raw = input("Issue period (days) [14]: ").strip()
    if days_raw == "":
        days = 14
    else:
        try:
            days = int(days_raw)
            if days < 1:
                print("Days must be >= 1")
                return
        except ValueError:
            print("Please enter a valid number of days.")
            return

    issue_date = datetime.date.today()
    due_date = issue_date + datetime.timedelta(days=days)

    cur.execute("INSERT INTO issues (member_id, book_id, issue_date, due_date) VALUES (%s,%s,%s,%s)", (member_id, book_id, issue_date, due_date))
    issue_id = cur.lastrowid
    cur.execute("UPDATE books SET stock = stock - 1 WHERE book_id=%s", (book_id,))
    con.commit()
    print(f"   Issued '{title}' (Book {book_id}) to {member_name} (Member #{member_id}).")
    print(f"   Issue ID: {issue_id} | Due on {due_date}")


def return_book(cur, con):
    print("-- Return Book --")
    issue_id = input_int("Issue ID: ", min_val=1)
    cur.execute("""
        SELECT i.issue_id, i.member_id, m.name, i.book_id, b.title, i.issue_date, i.due_date, i.return_date
        FROM issues i
        JOIN members m ON m.member_id = i.member_id
        JOIN books b   ON b.book_id   = i.book_id
        WHERE i.issue_id=%s
    """, (issue_id,))
    row = cur.fetchone()
    if not row:
        print("Issue record not found.")
        return
    if row[7] is not None:
        print("This book was already returned.")
        return

    _, member_id, member_name, book_id, title, _, due_date, _ = row
    return_date = datetime.date.today()
    late_fee = 0.0
    if return_date > due_date:
        days_late = (return_date - due_date).days
        late_fee = days_late * 5.0  # Rs.5 per day late fee

    cur.execute("UPDATE issues SET return_date=%s, late_fee=%s WHERE issue_id=%s", (return_date, late_fee, issue_id))
    cur.execute("UPDATE books SET stock = stock + 1 WHERE book_id=%s", (book_id,))
    con.commit()
    print(f"Returned '{title}' from {member_name} (Member #{member_id}). Late fee: Rs.{late_fee:.2f}")


def view_active_issues(cur):
    print("-- Active Issues (Not Yet Returned) --")
    cur.execute(
        """
        SELECT i.issue_id, m.name, m.member_id, b.title, b.book_id, i.issue_date, i.due_date
        FROM issues i
        JOIN members m ON m.member_id = i.member_id
        JOIN books b   ON b.book_id   = i.book_id
        WHERE i.return_date IS NULL
        ORDER BY i.issue_date DESC
        """
    )
    rows = cur.fetchall()
    if not rows:
        print("(none)")
        return
    for r in rows:
        print(f"Issue #{r[0]} | Member: {r[1]} (#{r[2]}) | Book: {r[3]} ({r[4]}) | Issued: {r[5]} | Due: {r[6]}")


def view_issues_by_month(cur):
    print("-- Issues by Month --")
    ym = input("Enter month (YYYY-MM): ").strip()
    try:
        start, end = parse_year_month(ym)
    except Exception:
        print("Invalid format. Example: 2025-08")
        return

    print("Filter by: 1) Issue Date  2) Return Date  3) Any")
    f = input("Choice [1/2/3]: ").strip() or '1'

    if f == '1':
        where = "i.issue_date BETWEEN %s AND %s"
        params = (start, end)
    elif f == '2':
        where = "i.return_date BETWEEN %s AND %s"
        params = (start, end)
    else:
        where = "(i.issue_date BETWEEN %s AND %s OR i.return_date BETWEEN %s AND %s)"
        params = (start, end, start, end)

    cur.execute(
        f"""
        SELECT i.issue_id, m.name, m.member_id, b.title, b.book_id,
               i.issue_date, i.due_date, i.return_date, i.late_fee
        FROM issues i
        JOIN members m ON m.member_id = i.member_id
        JOIN books b   ON b.book_id   = i.book_id
        WHERE {where}
        ORDER BY COALESCE(i.return_date, i.issue_date) DESC
        """,
        params,
    )
    rows = cur.fetchall()
    if not rows:
        print("(no records)")
        return
    for r in rows:
        status = "Returned" if r[7] else "Issued"
        print(f"Issue #{r[0]} | {status} | Member: {r[1]} (#{r[2]}) | Book: {r[3]} ({r[4]}) | Issue: {r[5]} | Due: {r[6]} | Return: {r[7] or '-'} | Late Fee: Rs.{float(r[8]):.2f}")

# -------------------- BILLING --------------------

def create_bill(cur, con):
    print("-- Create Bill --")
    member_id_input = input("Member ID (ENTER if none): ").strip()
    member_id = None
    member_type = 'Regular'
    member_name = 'Guest'
    if member_id_input:
        try:
            member_id = int(member_id_input)
            cur.execute("SELECT name, membership_type FROM members WHERE member_id=%s", (member_id,))
            row = cur.fetchone()
            if row:
                member_name, member_type = row[0], row[1]
            else:
                print("Member not found. Billing as guest.")
                member_id = None
        except ValueError:
            member_id = None

    items = []
    while True:
        book_id = input("Book ID (or ENTER to finish): ").strip()
        if book_id == "":
            break
        qty = input_int("Quantity: ", min_val=1)
        cur.execute("SELECT title, price, stock FROM books WHERE book_id=%s", (book_id,))
        row = cur.fetchone()
        if not row:
            print("Book not found.")
            continue
        title, price, stock = row
        if stock < qty:
            print(f"Not enough stock. Available: {stock}")
            continue
        line_total = float(price) * qty
        items.append({"book_id": book_id, "title": title, "qty": qty, "unit_price": float(price), "line_total": line_total})
        print(f"Added: {title} x{qty} = Rs.{line_total}")

    if not items:
        print("No items added. Bill cancelled.")
        return

    subtotal = sum(i['line_total'] for i in items)
    discount_pct = input_float("Discount % (0 for none): ", min_val=0, max_val=100)

    # VIP extra discount: VIP gets additional 10% off
    vip_extra = 10.0 if member_type == 'VIP' else 0.0
    if vip_extra:
        print("VIP member detected: +10% extra discount applied.")

    total_discount_pct = discount_pct + vip_extra
    # cap discount to 100%
    total_discount_pct = min(total_discount_pct, 100.0)
    discount_amt = subtotal * (total_discount_pct / 100.0)
    grand_total = max(subtotal - discount_amt, 0.0)

    cur.execute("INSERT INTO bills (member_id, bill_date, subtotal, discount_pct, discount_amt, grand_total) VALUES (%s,%s,%s,%s,%s,%s)", (member_id, datetime.datetime.now(), subtotal, total_discount_pct, discount_amt, grand_total))
    bill_id = cur.lastrowid

    for it in items:
        cur.execute("INSERT INTO bill_items (bill_id, book_id, qty, unit_price, line_total) VALUES (%s,%s,%s,%s,%s)", (bill_id, it['book_id'], it['qty'], it['unit_price'], it['line_total']))
        cur.execute("UPDATE books SET stock = stock - %s WHERE book_id=%s", (it['qty'], it['book_id']))

    con.commit()
    print("Bill saved.")
    print(f"Bill ID: {bill_id} | Customer: {member_name} ({member_type})")
    print(f"Subtotal: Rs.{subtotal:.2f}")
    print(f"Total Discount %: {total_discount_pct:.2f}% (Rs.{discount_amt:.2f})")
    print(f"Grand Total: Rs.{grand_total:.2f}")


def view_bills(cur):
    print("-- Recent Bills --")
    cur.execute("SELECT bill_id, member_id, bill_date, subtotal, discount_amt, grand_total FROM bills ORDER BY bill_id DESC LIMIT 20")
    rows = cur.fetchall()
    if not rows:
        print("(no bills)")
        return
    for r in rows:
        bid, mid, bdate, sub, damt, total = r
        print(f"#{bid} | Member: {mid or 'Guest'} | {bdate} | Sub: Rs.{sub} | Disc: Rs.{damt} | Total: Rs.{total}")


def view_bills_by_month(cur):
    print("-- Bills by Month --")
    ym = input("Enter month (YYYY-MM): ").strip()
    try:
        start, end = parse_year_month(ym)
    except Exception:
        print("Invalid format. Example: 2025-08")
        return
    cur.execute(
        """
        SELECT b.bill_id, b.bill_date, COALESCE(m.name,'Guest') AS customer, COALESCE(m.membership_type,'-') AS mtype,
               b.subtotal, b.discount_pct, b.discount_amt, b.grand_total
        FROM bills b
        LEFT JOIN members m ON m.member_id = b.member_id
        WHERE DATE(b.bill_date) BETWEEN %s AND %s
        ORDER BY b.bill_date DESC
        """,
        (start, end),
    )
    rows = cur.fetchall()
    if not rows:
        print("(no bills in this month)")
        return
    for r in rows:
        print(f"Bill #{r[0]} | {r[1]} | {r[2]} ({r[3]}) | Sub: {r[4]} | Disc%: {r[5]} | DiscAmt: {r[6]} | Total: {r[7]}")


def show_bill_details(cur):
    print("-- Bill Details --")
    bill_id = input_int("Enter Bill ID: ", min_val=1)
    cur.execute(
        """
        SELECT bi.item_id, bi.book_id, b.title, bi.qty, bi.unit_price, bi.line_total
        FROM bill_items bi
        JOIN books b ON b.book_id = bi.book_id
        WHERE bi.bill_id = %s
        ORDER BY bi.item_id
        """,
        (bill_id,),
    )
    rows = cur.fetchall()
    if not rows:
        print("(no items found for this bill)")
        return
    print(f"Bill #{bill_id} items:")
    print(f"{'#':<4} {'BookID':<10} {'Title':<30} {'Qty':>4} {'Unit':>8} {'Line':>10}")
    print('-' * 70)
    for r in rows:
        print(f"{r[0]:<4} {r[1]:<10} {r[2]:<30.30} {r[3]:>4} {r[4]:>8.2f} {r[5]:>10.2f}")

# -------------------- CSV EXPORT --------------------

def export_table_csv(cur, table_name, filename):
    """Raw table export with headers (validated)."""
    if table_name not in ALLOWED_TABLES:
        print("Invalid table name for export.")
        return
    try:
        cur.execute(f"SELECT * FROM {table_name}")
    except Exception as e:
        print("Failed to fetch data:", e)
        return
    rows = cur.fetchall()
    cols = [d[0] for d in cur.description]
    if not rows:
        print("(no data to export)")
        return
    try:
        with open(filename, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(cols)
            writer.writerows(rows)
        print(f"Exported {table_name} to {filename}")
    except Exception as e:
        print("Failed to write CSV:", e)


def export_issues_detailed_csv(cur, filename):
    cur.execute(
        """
        SELECT i.issue_id,
               m.member_id, m.name AS member_name, m.membership_type,
               b.book_id, b.title AS book_title,
               i.issue_date, i.due_date, i.return_date, i.late_fee
        FROM issues i
        JOIN members m ON m.member_id = i.member_id
        JOIN books b   ON b.book_id   = i.book_id
        ORDER BY i.issue_id DESC
        """
    )
    rows = cur.fetchall()
    cols = ["issue_id","member_id","member_name","membership_type","book_id","book_title","issue_date","due_date","return_date","late_fee"]
    if not rows:
        print("(no issues to export)")
        return
    with open(filename, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(cols)
        writer.writerows(rows)
    print(f"Exported detailed Issues to {filename}")


def export_bills_detailed_csv(cur, filename):
    cur.execute(
        """
        SELECT b.bill_id, DATE(b.bill_date) AS bill_date, TIME(b.bill_date) AS bill_time,
               COALESCE(m.name,'Guest') AS customer, COALESCE(m.membership_type,'-') AS membership_type,
               b.subtotal, b.discount_pct, b.discount_amt, b.grand_total
        FROM bills b
        LEFT JOIN members m ON m.member_id = b.member_id
        ORDER BY b.bill_id DESC
        """
    )
    rows = cur.fetchall()
    cols = ["bill_id","bill_date","bill_time","customer","membership_type","subtotal","discount_pct","discount_amt","grand_total"]
    if not rows:
        print("(no bills to export)")
        return
    with open(filename, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(cols)
        writer.writerows(rows)
    print(f"Exported detailed Bills to {filename}")

# -------------------- MENUS --------------------

def books_menu(cur, con):
    while True:
        print("=== Books Menu ===")
        print("1. Add Book")
        print("2. Update Book")
        print("3. Delete Book")
        print("4. View Books")
        print("5. Search Books")
        print("6. Export Books to CSV")
        print("7. Back")
        choice = input("Choice: ").strip()
        if choice == "1":
            add_book(cur, con)
        elif choice == "2":
            update_book(cur, con)
        elif choice == "3":
            delete_book(cur, con)
        elif choice == "4":
            view_books(cur)
        elif choice == "5":
            search_books(cur)
        elif choice == "6":
            fname = input("Filename (e.g., books.csv): ").strip() or 'books.csv'
            export_table_csv(cur, 'books', fname)
        elif choice == "7":
            break
        else:
            print("Invalid choice.")


def staff_menu(cur, con):
    while True:
        print("=== Staff Menu ===")
        print("1. Add Staff")
        print("2. Update Staff")
        print("3. Delete Staff")
        print("4. View Staff")
        print("5. Export Staff to CSV")
        print("6. Back")
        choice = input("Choice: ").strip()
        if choice == "1":
            add_staff(cur, con)
        elif choice == "2":
            update_staff(cur, con)
        elif choice == "3":
            delete_staff(cur, con)
        elif choice == "4":
            view_staff(cur)
        elif choice == "5":
            fname = input("Filename (e.g., staff.csv): ").strip() or 'staff.csv'
            export_table_csv(cur, 'staff', fname)
        elif choice == "6":
            break
        else:
            print("Invalid choice.")


def members_menu(cur, con):
    while True:
        print("=== Members / Issue-Return Menu ===")
        print("1. Add Member")
        print("2. Update Member")
        print("3. Delete Member")
        print("4. View Members")
        print("5. Issue Book")
        print("6. Return Book")
        print("7. View Active Issues")
        print("8. View Issues by Month")
        print("9. Export Issues (Detailed CSV)")
        print("10. Export Members to CSV")
        print("11. Back")
        choice = input("Choice: ").strip()
        if choice == "1":
            add_member(cur, con)
        elif choice == "2":
            update_member(cur, con)
        elif choice == "3":
            delete_member(cur, con)
        elif choice == "4":
            view_members(cur)
        elif choice == "5":
            issue_book(cur, con)
        elif choice == "6":
            return_book(cur, con)
        elif choice == "7":
            view_active_issues(cur)
        elif choice == "8":
            view_issues_by_month(cur)
        elif choice == "9":
            fname = input("Filename (e.g., issues_detailed.csv): ").strip() or 'issues_detailed.csv'
            export_issues_detailed_csv(cur, fname)
        elif choice == "10":
            fname = input("Filename (e.g., members.csv): ").strip() or 'members.csv'
            export_table_csv(cur, 'members', fname)
        elif choice == "11":
            break
        else:
            print("Invalid choice.")


def billing_menu(cur, con):
    while True:
        print("=== Billing Menu ===")
        print("1. Create Bill")
        print("2. View Recent Bills")
        print("3. View Bills by Month")
        print("4. View Bill Details")
        print("5. Export Bills (Detailed CSV)")
        print("6. Back")
        choice = input("Choice: ").strip()
        if choice == "1":
            create_bill(cur, con)
        elif choice == "2":
            view_bills(cur)
        elif choice == "3":
            view_bills_by_month(cur)
        elif choice == "4":
            show_bill_details(cur)
        elif choice == "5":
            fname = input("Filename (e.g., bills_detailed.csv): ").strip() or 'bills_detailed.csv'
            export_bills_detailed_csv(cur, fname)
        elif choice == "6":
            break
        else:
            print("Invalid choice.")

# -------------------- MAIN --------------------

def main():
    try:
        init_database_and_tables()
    except Exception as e:
        print("Failed to initialize database:", e)
        return

    try:
        con = get_connection(use_db=True)
    except Exception as e:
        print("Could not connect to database. Check DB_CONFIG.", e)
        return

    cur = con.cursor()

    while True:
        print("==============================")
        print(" Library Management System ")
        print("==============================")
        print("1. Books")
        print("2. Staff")
        print("3. Members / Issue-Return")
        print("4. Billing")
        print("5. Exit")
        choice = input("Choice: ").strip()
        if choice == "1":
            books_menu(cur, con)
        elif choice == "2":
            staff_menu(cur, con)
        elif choice == "3":
            members_menu(cur, con)
        elif choice == "4":
            billing_menu(cur, con)
        elif choice == "5":
            break
        else:
            print("Invalid choice.")

    cur.close()
    con.close()
    print("Goodbye!")


if __name__ == "__main__":
    main()
