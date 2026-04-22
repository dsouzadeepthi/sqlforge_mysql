const PATTERNS = [
  { id: 1, name: "Top N per Group", icon: "🏆", template: "ROW_NUMBER() OVER (PARTITION BY group ORDER BY metric DESC)" },
  { id: 2, name: "Deduplication", icon: "🧹", template: "ROW_NUMBER() OVER (PARTITION BY key ORDER BY date DESC) = 1" },
  { id: 3, name: "Running Total", icon: "📈", template: "SUM(amount) OVER (PARTITION BY key ORDER BY date)" },
  { id: 4, name: "Moving Average", icon: "〰️", template: "AVG(val) OVER (PARTITION BY key ORDER BY date ROWS BETWEEN 2 PRECEDING AND CURRENT ROW)" },
  { id: 5, name: "Next / Previous Row", icon: "↕️", template: "LAG(col) OVER (...) / LEAD(col) OVER (...)" },
  { id: 6, name: "Day-1 Retention", icon: "🔄", template: "LEAD(date) = date + INTERVAL 1 DAY" },
  { id: 7, name: "Sessionization", icon: "📦", template: "SUM(new_session_flag) OVER (PARTITION BY user ORDER BY time)" },
  { id: 8, name: "Consecutive Streaks", icon: "🔥", template: "date - ROW_NUMBER() → same streak group" },
  { id: 9, name: "Gaps Between Events", icon: "⏱️", template: "event_time - LAG(event_time) OVER (...)" },
  { id: 10, name: "First / Last Value", icon: "🎯", template: "FIRST_VALUE(col) OVER (...) / LAST_VALUE(col) OVER (...)" },
  { id: 11, name: "Percent of Total", icon: "🥧", template: "amount / SUM(amount) OVER ()" },
  { id: 12, name: "Ranking Variants", icon: "🎖️", template: "RANK() / DENSE_RANK() / ROW_NUMBER() OVER (...)" },
  { id: 13, name: "Conditional Aggregation", icon: "🔀", template: "SUM(CASE WHEN condition THEN 1 ELSE 0 END)" },
  { id: 14, name: "Funnel Analysis", icon: "🔽", template: "COUNT(DISTINCT CASE WHEN step='A' THEN user END)" },
  { id: 15, name: "Self Join for Pairs", icon: "🤝", template: "a.user_id = b.user_id AND a.id < b.id" },
  { id: 16, name: "Anti Join (NOT EXISTS)", icon: "🚫", template: "WHERE NOT EXISTS (SELECT 1 FROM ... WHERE ...)" },
  { id: 17, name: "Cohort Analysis", icon: "👥", template: "DATE_FORMAT(signup_date, '%Y-%m-01')" },
  { id: 18, name: "Window Filtering", icon: "🪟", template: "SELECT * FROM (SELECT ..., ROW_NUMBER() OVER (...)) t WHERE rn = 1" },
  { id: 19, name: "All Conditions Must Hold", icon: "✅", template: "HAVING SUM(violation_flag) = 0" },
  { id: 20, name: "Rolling Retention / Activity", icon: "📅", template: "COUNT(DISTINCT user_id) OVER (ORDER BY date ROWS BETWEEN X PRECEDING AND CURRENT ROW)" }
];

const QUESTIONS = [
  // ─── PATTERN 1: Top N per Group ───────────────────────────────────────────
  {
    id: 1, pattern: 1, difficulty: "Easy",
    title: "Top 2 Earners per Department",
    description: `You have an <code>employees</code> table. Write a query to find the <strong>top 2 highest-paid employees</strong> in each department.\n\nReturn: <code>department</code>, <code>name</code>, <code>salary</code>`,
    schema: `CREATE TABLE employees (emp_id INT, name TEXT, department TEXT, salary INT);
INSERT INTO employees VALUES
(1,'Alice','Engineering',95000),(2,'Bob','Engineering',88000),
(3,'Carol','Engineering',92000),(4,'David','Marketing',78000),
(5,'Eve','Marketing',82000),(6,'Frank','Marketing',75000),
(7,'Grace','HR',70000),(8,'Henry','HR',68000),(9,'Iris','HR',73000);`,
    hint: "Use ROW_NUMBER() OVER (PARTITION BY department ORDER BY salary DESC) and filter where rn <= 2",
    solution: `SELECT department, name, salary FROM (
  SELECT *, ROW_NUMBER() OVER (PARTITION BY department ORDER BY salary DESC) AS rn
  FROM employees
) t WHERE rn <= 2 ORDER BY department, salary DESC;`
  },
  {
    id: 2, pattern: 1, difficulty: "Easy",
    title: "Top 3 Products by Revenue per Category",
    description: `You have a <code>products</code> table. Find the <strong>top 3 products by revenue</strong> in each category.\n\nReturn: <code>category</code>, <code>product_name</code>, <code>revenue</code>`,
    schema: `CREATE TABLE products (product_id INT, product_name TEXT, category TEXT, revenue INT);
INSERT INTO products VALUES
(1,'Laptop','Electronics',50000),(2,'Phone','Electronics',45000),
(3,'Tablet','Electronics',30000),(4,'Headphones','Electronics',15000),
(5,'T-Shirt','Clothing',8000),(6,'Jeans','Clothing',12000),
(7,'Jacket','Clothing',20000),(8,'Sneakers','Clothing',18000),
(9,'Desk','Furniture',25000),(10,'Chair','Furniture',22000),(11,'Lamp','Furniture',5000);`,
    hint: "PARTITION BY category, ORDER BY revenue DESC, filter rn <= 3",
    solution: `SELECT category, product_name, revenue FROM (
  SELECT *, ROW_NUMBER() OVER (PARTITION BY category ORDER BY revenue DESC) AS rn
  FROM products
) t WHERE rn <= 3 ORDER BY category, revenue DESC;`
  },
  {
    id: 3, pattern: 1, difficulty: "Medium",
    title: "Top Scoring Student per Class",
    description: `You have a <code>students</code> table. Find the <strong>single top-scoring student</strong> (by score) per class.\n\nReturn: <code>class</code>, <code>student_name</code>, <code>score</code>`,
    schema: `CREATE TABLE students (student_id INT, student_name TEXT, class TEXT, score INT);
INSERT INTO students VALUES
(1,'Alice','Math',95),(2,'Bob','Math',92),(3,'Carol','Math',88),
(4,'David','Science',97),(5,'Eve','Science',90),(6,'Frank','Science',85),
(7,'Grace','History',89),(8,'Henry','History',94),(9,'Iris','History',91);`,
    hint: "Filter WHERE rn = 1 to get only the top scorer per class",
    solution: `SELECT class, student_name, score FROM (
  SELECT *, ROW_NUMBER() OVER (PARTITION BY class ORDER BY score DESC) AS rn
  FROM students
) t WHERE rn = 1 ORDER BY class;`
  },
  {
    id: 4, pattern: 1, difficulty: "Medium",
    title: "Top 2 Most Expensive Orders per Customer",
    description: `You have an <code>orders</code> table. Find the <strong>top 2 most expensive orders</strong> per customer.\n\nReturn: <code>customer_id</code>, <code>order_id</code>, <code>amount</code>`,
    schema: `CREATE TABLE orders (order_id INT, customer_id INT, amount DECIMAL, order_date TEXT);
INSERT INTO orders VALUES
(1,101,250.00,'2024-01-01'),(2,101,180.00,'2024-01-05'),(3,101,320.00,'2024-01-10'),
(4,102,450.00,'2024-01-02'),(5,102,90.00,'2024-01-08'),(6,102,200.00,'2024-01-12'),
(7,103,150.00,'2024-01-03'),(8,103,380.00,'2024-01-07');`,
    hint: "Partition by customer_id, order by amount DESC, keep rn <= 2",
    solution: `SELECT customer_id, order_id, amount FROM (
  SELECT *, ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY amount DESC) AS rn
  FROM orders
) t WHERE rn <= 2 ORDER BY customer_id, amount DESC;`
  },
  {
    id: 5, pattern: 1, difficulty: "Hard",
    title: "Top 2 Sales Reps by Revenue per Region and Quarter",
    description: `You have a <code>sales</code> table. Find the <strong>top 2 sales reps by total revenue</strong> for each region and quarter combination.\n\nReturn: <code>region</code>, <code>quarter</code>, <code>rep_name</code>, <code>total_revenue</code>`,
    schema: `CREATE TABLE sales (sale_id INT, rep_name TEXT, region TEXT, quarter TEXT, revenue INT);
INSERT INTO sales VALUES
(1,'Alice','North','Q1',12000),(2,'Bob','North','Q1',15000),(3,'Carol','North','Q1',9000),
(4,'David','North','Q2',11000),(5,'Eve','North','Q2',14000),(6,'Frank','North','Q2',13000),
(7,'Grace','South','Q1',18000),(8,'Henry','South','Q1',16000),(9,'Iris','South','Q1',14000),
(10,'Jack','South','Q2',20000),(11,'Kate','South','Q2',17000),(12,'Leo','South','Q2',15000);`,
    hint: "First aggregate by rep/region/quarter, then apply ROW_NUMBER on the aggregated result",
    solution: `WITH rep_totals AS (
  SELECT region, quarter, rep_name, SUM(revenue) AS total_revenue
  FROM sales
  GROUP BY region, quarter, rep_name
),
ranked AS (
  SELECT *, ROW_NUMBER() OVER (PARTITION BY region, quarter ORDER BY total_revenue DESC) AS rn
  FROM rep_totals
)
SELECT region, quarter, rep_name, total_revenue FROM ranked WHERE rn <= 2 ORDER BY region, quarter, total_revenue DESC;`
  },

  // ─── PATTERN 2: Deduplication ─────────────────────────────────────────────
  {
    id: 6, pattern: 2, difficulty: "Easy",
    title: "Latest User Profile",
    description: `The <code>user_profiles</code> table has multiple records per user due to updates. Return only the <strong>most recent profile</strong> for each user.\n\nReturn: <code>user_id</code>, <code>email</code>, <code>updated_at</code>`,
    schema: `CREATE TABLE user_profiles (record_id INT, user_id INT, email TEXT, updated_at TEXT);
INSERT INTO user_profiles VALUES
(1,101,'alice@old.com','2024-01-01'),(2,101,'alice@new.com','2024-03-15'),
(3,102,'bob@email.com','2024-02-10'),(4,102,'bob@work.com','2024-04-01'),
(5,103,'carol@email.com','2024-01-20'),(6,103,'carol@personal.com','2024-03-01');`,
    hint: "ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY updated_at DESC) = 1",
    solution: `SELECT user_id, email, updated_at FROM (
  SELECT *, ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY updated_at DESC) AS rn
  FROM user_profiles
) t WHERE rn = 1;`
  },
  {
    id: 7, pattern: 2, difficulty: "Easy",
    title: "Current Order Status",
    description: `The <code>order_events</code> table tracks status changes for orders. Return the <strong>latest status</strong> for each order.\n\nReturn: <code>order_id</code>, <code>status</code>, <code>event_time</code>`,
    schema: `CREATE TABLE order_events (event_id INT, order_id INT, status TEXT, event_time TEXT);
INSERT INTO order_events VALUES
(1,501,'placed','2024-05-01 10:00'),(2,501,'shipped','2024-05-02 14:00'),(3,501,'delivered','2024-05-05 09:00'),
(4,502,'placed','2024-05-01 11:00'),(5,502,'cancelled','2024-05-03 16:00'),
(6,503,'placed','2024-05-02 08:00'),(7,503,'shipped','2024-05-04 12:00');`,
    hint: "PARTITION BY order_id, ORDER BY event_time DESC, keep rn = 1",
    solution: `SELECT order_id, status, event_time FROM (
  SELECT *, ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY event_time DESC) AS rn
  FROM order_events
) t WHERE rn = 1;`
  },
  {
    id: 8, pattern: 2, difficulty: "Medium",
    title: "Latest Product Price",
    description: `The <code>price_history</code> table has a log of price changes per product. Return the <strong>current (latest) price</strong> for each product.\n\nReturn: <code>product_id</code>, <code>price</code>, <code>effective_date</code>`,
    schema: `CREATE TABLE price_history (id INT, product_id INT, price DECIMAL, effective_date TEXT);
INSERT INTO price_history VALUES
(1,1,99.99,'2024-01-01'),(2,1,89.99,'2024-02-01'),(3,1,79.99,'2024-03-01'),
(4,2,49.99,'2024-01-01'),(5,2,54.99,'2024-02-15'),
(6,3,199.99,'2024-01-01'),(7,3,189.99,'2024-03-10'),(8,3,179.99,'2024-04-01');`,
    hint: "Deduplicate by product_id keeping the row with the most recent effective_date",
    solution: `SELECT product_id, price, effective_date FROM (
  SELECT *, ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY effective_date DESC) AS rn
  FROM price_history
) t WHERE rn = 1;`
  },
  {
    id: 9, pattern: 2, difficulty: "Medium",
    title: "Deduplicate Duplicate Signups",
    description: `The <code>signups</code> table may have duplicate email registrations. Keep only the <strong>earliest signup</strong> per email.\n\nReturn: <code>email</code>, <code>name</code>, <code>signup_date</code>`,
    schema: `CREATE TABLE signups (signup_id INT, email TEXT, name TEXT, signup_date TEXT);
INSERT INTO signups VALUES
(1,'alice@test.com','Alice A','2024-01-05'),(2,'alice@test.com','Alice B','2024-01-10'),
(3,'bob@test.com','Bob','2024-01-08'),(4,'carol@test.com','Carol X','2024-01-02'),
(5,'carol@test.com','Carol Y','2024-01-15'),(6,'david@test.com','David','2024-01-20');`,
    hint: "ORDER BY signup_date ASC (not DESC) to keep the earliest row",
    solution: `SELECT email, name, signup_date FROM (
  SELECT *, ROW_NUMBER() OVER (PARTITION BY email ORDER BY signup_date ASC) AS rn
  FROM signups
) t WHERE rn = 1;`
  },
  {
    id: 10, pattern: 2, difficulty: "Hard",
    title: "Latest Non-Null Value per User",
    description: `The <code>user_settings</code> table has sparse updates — some rows have NULL values for <code>theme</code>. Return the <strong>most recent non-null theme</strong> per user (if any).\n\nReturn: <code>user_id</code>, <code>theme</code>, <code>updated_at</code>`,
    schema: `CREATE TABLE user_settings (id INT, user_id INT, theme TEXT, updated_at TEXT);
INSERT INTO user_settings VALUES
(1,101,'dark','2024-01-01'),(2,101,NULL,'2024-02-01'),(3,101,NULL,'2024-03-01'),
(4,102,'light','2024-01-15'),(5,102,'dark','2024-02-15'),
(6,103,NULL,'2024-01-20'),(7,103,NULL,'2024-03-20');`,
    hint: "Filter out NULLs before applying ROW_NUMBER, then get rn = 1",
    solution: `SELECT user_id, theme, updated_at FROM (
  SELECT *, ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY updated_at DESC) AS rn
  FROM user_settings
  WHERE theme IS NOT NULL
) t WHERE rn = 1;`
  },

  // ─── PATTERN 3: Running Total ─────────────────────────────────────────────
  {
    id: 11, pattern: 3, difficulty: "Easy",
    title: "Running Transaction Total per User",
    description: `You have a <code>transactions</code> table. Show the <strong>running total of amount</strong> per user, ordered by date.\n\nReturn: <code>user_id</code>, <code>txn_date</code>, <code>amount</code>, <code>running_total</code>`,
    schema: `CREATE TABLE transactions (txn_id INT, user_id INT, amount INT, txn_date TEXT);
INSERT INTO transactions VALUES
(1,101,100,'2024-01-01'),(2,101,150,'2024-01-03'),(3,101,200,'2024-01-07'),
(4,102,50,'2024-01-02'),(5,102,300,'2024-01-05'),(6,102,75,'2024-01-09');`,
    hint: "SUM(amount) OVER (PARTITION BY user_id ORDER BY txn_date)",
    solution: `SELECT user_id, txn_date, amount,
  SUM(amount) OVER (PARTITION BY user_id ORDER BY txn_date) AS running_total
FROM transactions ORDER BY user_id, txn_date;`
  },
  {
    id: 12, pattern: 3, difficulty: "Easy",
    title: "Cumulative Revenue per Region",
    description: `You have a <code>daily_sales</code> table. Show the <strong>cumulative revenue per region</strong> over time.\n\nReturn: <code>region</code>, <code>sale_date</code>, <code>revenue</code>, <code>cumulative_revenue</code>`,
    schema: `CREATE TABLE daily_sales (sale_id INT, region TEXT, revenue INT, sale_date TEXT);
INSERT INTO daily_sales VALUES
(1,'North',1000,'2024-01-01'),(2,'North',1500,'2024-01-02'),(3,'North',800,'2024-01-03'),
(4,'South',2000,'2024-01-01'),(5,'South',500,'2024-01-02'),(6,'South',1200,'2024-01-03');`,
    hint: "SUM(revenue) OVER (PARTITION BY region ORDER BY sale_date)",
    solution: `SELECT region, sale_date, revenue,
  SUM(revenue) OVER (PARTITION BY region ORDER BY sale_date) AS cumulative_revenue
FROM daily_sales ORDER BY region, sale_date;`
  },
  {
    id: 13, pattern: 3, difficulty: "Medium",
    title: "Running Balance (Credits & Debits)",
    description: `You have a <code>payments</code> table with <code>credit</code> and <code>debit</code> types. Show a <strong>running balance per user</strong> where credits add and debits subtract.\n\nReturn: <code>user_id</code>, <code>payment_date</code>, <code>type</code>, <code>amount</code>, <code>running_balance</code>`,
    schema: `CREATE TABLE payments (payment_id INT, user_id INT, amount INT, payment_date TEXT, type TEXT);
INSERT INTO payments VALUES
(1,101,200,'2024-01-01','credit'),(2,101,50,'2024-01-03','debit'),(3,101,100,'2024-01-06','credit'),
(4,102,300,'2024-01-02','credit'),(5,102,75,'2024-01-04','debit'),(6,102,120,'2024-01-08','credit');`,
    hint: "Use SUM(CASE WHEN type='credit' THEN amount ELSE -amount END) OVER (...)",
    solution: `SELECT user_id, payment_date, type, amount,
  SUM(CASE WHEN type='credit' THEN amount ELSE -amount END)
    OVER (PARTITION BY user_id ORDER BY payment_date) AS running_balance
FROM payments ORDER BY user_id, payment_date;`
  },
  {
    id: 14, pattern: 3, difficulty: "Medium",
    title: "Running Count of Orders per Customer",
    description: `You have an <code>orders</code> table. Show a <strong>running count of orders</strong> per customer over time.\n\nReturn: <code>customer_id</code>, <code>order_date</code>, <code>order_id</code>, <code>order_number</code>`,
    schema: `CREATE TABLE cust_orders (order_id INT, customer_id INT, order_date TEXT, amount INT);
INSERT INTO cust_orders VALUES
(1,101,'2024-01-05',120),(2,101,'2024-01-15',200),(3,101,'2024-02-01',85),
(4,102,'2024-01-10',300),(5,102,'2024-01-20',150),(6,103,'2024-01-08',90),(7,103,'2024-02-05',210);`,
    hint: "COUNT(*) OVER (PARTITION BY customer_id ORDER BY order_date) gives the running order count",
    solution: `SELECT customer_id, order_date, order_id,
  COUNT(*) OVER (PARTITION BY customer_id ORDER BY order_date) AS order_number
FROM cust_orders ORDER BY customer_id, order_date;`
  },
  {
    id: 15, pattern: 3, difficulty: "Hard",
    title: "Running Stock Balance with Low Stock Alert",
    description: `You have a <code>stock_movements</code> table with <code>inbound</code> and <code>outbound</code> movements. Show the running stock balance per product. Also add a <code>stock_alert</code> column: <code>'Low Stock'</code> if balance < 80, else <code>'OK'</code>.\n\nReturn: <code>product_id</code>, <code>move_date</code>, <code>movement_type</code>, <code>quantity</code>, <code>running_balance</code>, <code>stock_alert</code>`,
    schema: `CREATE TABLE stock_movements (move_id INT, product_id TEXT, movement_type TEXT, quantity INT, move_date TEXT);
INSERT INTO stock_movements VALUES
(1,'P01','inbound',100,'2024-01-01'),(2,'P01','outbound',30,'2024-01-03'),
(3,'P01','inbound',50,'2024-01-05'),(4,'P01','outbound',60,'2024-01-07'),
(5,'P02','inbound',200,'2024-01-01'),(6,'P02','outbound',80,'2024-01-04'),(7,'P02','outbound',70,'2024-01-06');`,
    hint: "Use a CTE to compute the running balance, then wrap with CASE WHEN for the alert",
    solution: `WITH running AS (
  SELECT *,
    SUM(CASE WHEN movement_type='inbound' THEN quantity ELSE -quantity END)
      OVER (PARTITION BY product_id ORDER BY move_date) AS running_balance
  FROM stock_movements
)
SELECT product_id, move_date, movement_type, quantity, running_balance,
  CASE WHEN running_balance < 80 THEN 'Low Stock' ELSE 'OK' END AS stock_alert
FROM running ORDER BY product_id, move_date;`
  },

  // ─── PATTERN 4: Moving Average ────────────────────────────────────────────
  {
    id: 16, pattern: 4, difficulty: "Easy",
    title: "3-Day Moving Average of Sales",
    description: `You have a <code>daily_revenue</code> table. Calculate the <strong>3-day moving average</strong> of revenue (current day + 2 preceding days).\n\nReturn: <code>sale_date</code>, <code>revenue</code>, <code>moving_avg</code>`,
    schema: `CREATE TABLE daily_revenue (day_id INT, sale_date TEXT, revenue INT);
INSERT INTO daily_revenue VALUES
(1,'2024-01-01',1000),(2,'2024-01-02',1200),(3,'2024-01-03',900),
(4,'2024-01-04',1500),(5,'2024-01-05',800),(6,'2024-01-06',1100),(7,'2024-01-07',1300);`,
    hint: "AVG(revenue) OVER (ORDER BY sale_date ROWS BETWEEN 2 PRECEDING AND CURRENT ROW)",
    solution: `SELECT sale_date, revenue,
  ROUND(AVG(revenue) OVER (ORDER BY sale_date ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), 2) AS moving_avg
FROM daily_revenue ORDER BY sale_date;`
  },
  {
    id: 17, pattern: 4, difficulty: "Easy",
    title: "3-Day Moving Average per Product",
    description: `You have a <code>product_sales</code> table. Calculate the <strong>3-day moving average of units sold per product</strong>.\n\nReturn: <code>product_id</code>, <code>sale_date</code>, <code>units_sold</code>, <code>moving_avg_units</code>`,
    schema: `CREATE TABLE product_sales (id INT, product_id TEXT, sale_date TEXT, units_sold INT);
INSERT INTO product_sales VALUES
(1,'A','2024-01-01',10),(2,'A','2024-01-02',15),(3,'A','2024-01-03',12),(4,'A','2024-01-04',20),(5,'A','2024-01-05',8),
(6,'B','2024-01-01',5),(7,'B','2024-01-02',8),(8,'B','2024-01-03',6),(9,'B','2024-01-04',10),(10,'B','2024-01-05',7);`,
    hint: "PARTITION BY product_id, ORDER BY sale_date, ROWS BETWEEN 2 PRECEDING AND CURRENT ROW",
    solution: `SELECT product_id, sale_date, units_sold,
  ROUND(AVG(units_sold) OVER (PARTITION BY product_id ORDER BY sale_date ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), 2) AS moving_avg_units
FROM product_sales ORDER BY product_id, sale_date;`
  },
  {
    id: 18, pattern: 4, difficulty: "Medium",
    title: "7-Day Moving Average of Signups",
    description: `You have a <code>signups_daily</code> table. Calculate the <strong>7-day moving average</strong> of new user signups.\n\nReturn: <code>signup_date</code>, <code>new_users</code>, <code>moving_avg_7d</code>`,
    schema: `CREATE TABLE signups_daily (id INT, signup_date TEXT, new_users INT);
INSERT INTO signups_daily VALUES
(1,'2024-01-01',50),(2,'2024-01-02',60),(3,'2024-01-03',45),(4,'2024-01-04',70),
(5,'2024-01-05',55),(6,'2024-01-06',80),(7,'2024-01-07',65),(8,'2024-01-08',90),
(9,'2024-01-09',75),(10,'2024-01-10',85);`,
    hint: "ROWS BETWEEN 6 PRECEDING AND CURRENT ROW gives a 7-day window",
    solution: `SELECT signup_date, new_users,
  ROUND(AVG(new_users) OVER (ORDER BY signup_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2) AS moving_avg_7d
FROM signups_daily ORDER BY signup_date;`
  },
  {
    id: 19, pattern: 4, difficulty: "Medium",
    title: "Moving Average vs Actual (Flag Anomalies)",
    description: `You have a <code>metrics</code> table. Calculate the <strong>3-day moving average</strong> and flag rows where the actual value is <strong>more than 20% above the moving average</strong> as <code>'Spike'</code>, else <code>'Normal'</code>.\n\nReturn: <code>metric_date</code>, <code>value</code>, <code>moving_avg</code>, <code>status</code>`,
    schema: `CREATE TABLE metrics (id INT, metric_date TEXT, value INT);
INSERT INTO metrics VALUES
(1,'2024-01-01',100),(2,'2024-01-02',105),(3,'2024-01-03',102),
(4,'2024-01-04',180),(5,'2024-01-05',108),(6,'2024-01-06',103),(7,'2024-01-07',99);`,
    hint: "Compute moving_avg in a CTE, then use CASE WHEN value > moving_avg * 1.2 in outer query",
    solution: `WITH avg_calc AS (
  SELECT metric_date, value,
    ROUND(AVG(value) OVER (ORDER BY metric_date ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), 2) AS moving_avg
  FROM metrics
)
SELECT metric_date, value, moving_avg,
  CASE WHEN value > moving_avg * 1.2 THEN 'Spike' ELSE 'Normal' END AS status
FROM avg_calc ORDER BY metric_date;`
  },
  {
    id: 20, pattern: 4, difficulty: "Hard",
    title: "Centered Moving Average (Both Sides)",
    description: `You have a <code>temps</code> table of daily temperatures. Calculate a <strong>5-day centered moving average</strong> (2 preceding, current, 2 following rows).\n\nReturn: <code>temp_date</code>, <code>temperature</code>, <code>centered_avg</code>`,
    schema: `CREATE TABLE temps (id INT, temp_date TEXT, temperature DECIMAL);
INSERT INTO temps VALUES
(1,'2024-01-01',18.5),(2,'2024-01-02',19.0),(3,'2024-01-03',17.5),(4,'2024-01-04',20.0),
(5,'2024-01-05',22.0),(6,'2024-01-06',21.5),(7,'2024-01-07',19.5),(8,'2024-01-08',18.0);`,
    hint: "ROWS BETWEEN 2 PRECEDING AND 2 FOLLOWING gives a centered window",
    solution: `SELECT temp_date, temperature,
  ROUND(AVG(temperature) OVER (ORDER BY temp_date ROWS BETWEEN 2 PRECEDING AND 2 FOLLOWING), 2) AS centered_avg
FROM temps ORDER BY temp_date;`
  },

  // ─── PATTERN 5: Next / Previous Row ───────────────────────────────────────
  {
    id: 21, pattern: 5, difficulty: "Easy",
    title: "Day-over-Day Revenue Change",
    description: `You have a <code>revenue</code> table. Show each day's revenue alongside the <strong>previous day's revenue</strong> and the <strong>difference</strong>.\n\nReturn: <code>rev_date</code>, <code>revenue</code>, <code>prev_revenue</code>, <code>change</code>`,
    schema: `CREATE TABLE revenue (id INT, rev_date TEXT, revenue INT);
INSERT INTO revenue VALUES
(1,'2024-01-01',1000),(2,'2024-01-02',1200),(3,'2024-01-03',1100),
(4,'2024-01-04',1400),(5,'2024-01-05',1300),(6,'2024-01-06',1600);`,
    hint: "LAG(revenue, 1) OVER (ORDER BY rev_date) gives previous row's value",
    solution: `SELECT rev_date, revenue,
  LAG(revenue) OVER (ORDER BY rev_date) AS prev_revenue,
  revenue - LAG(revenue) OVER (ORDER BY rev_date) AS change
FROM revenue ORDER BY rev_date;`
  },
  {
    id: 22, pattern: 5, difficulty: "Easy",
    title: "Next Order Date per Customer",
    description: `You have an <code>orders</code> table. For each order, show the <strong>next order date</strong> for that customer.\n\nReturn: <code>customer_id</code>, <code>order_id</code>, <code>order_date</code>, <code>next_order_date</code>`,
    schema: `CREATE TABLE corders (order_id INT, customer_id INT, order_date TEXT);
INSERT INTO corders VALUES
(1,101,'2024-01-05'),(2,101,'2024-01-20'),(3,101,'2024-02-10'),
(4,102,'2024-01-08'),(5,102,'2024-02-01'),
(6,103,'2024-01-15'),(7,103,'2024-01-25'),(8,103,'2024-02-15');`,
    hint: "LEAD(order_date) OVER (PARTITION BY customer_id ORDER BY order_date)",
    solution: `SELECT customer_id, order_id, order_date,
  LEAD(order_date) OVER (PARTITION BY customer_id ORDER BY order_date) AS next_order_date
FROM corders ORDER BY customer_id, order_date;`
  },
  {
    id: 23, pattern: 5, difficulty: "Medium",
    title: "Month-over-Month Growth Percentage",
    description: `You have a <code>monthly_revenue</code> table. Calculate the <strong>month-over-month growth percentage</strong>.\n\nReturn: <code>month</code>, <code>revenue</code>, <code>prev_revenue</code>, <code>growth_pct</code>`,
    schema: `CREATE TABLE monthly_revenue (id INT, month TEXT, revenue INT);
INSERT INTO monthly_revenue VALUES
(1,'2024-01',10000),(2,'2024-02',12000),(3,'2024-03',11000),
(4,'2024-04',14000),(5,'2024-05',15500),(6,'2024-06',14000);`,
    hint: "Use LAG to get prev_revenue, then ROUND((revenue - prev) * 100.0 / prev, 2) for percentage",
    solution: `WITH lagged AS (
  SELECT month, revenue,
    LAG(revenue) OVER (ORDER BY month) AS prev_revenue
  FROM monthly_revenue
)
SELECT month, revenue, prev_revenue,
  ROUND((revenue - prev_revenue) * 100.0 / prev_revenue, 2) AS growth_pct
FROM lagged ORDER BY month;`
  },
  {
    id: 24, pattern: 5, difficulty: "Medium",
    title: "Compare Employee Salary to Previous Hire",
    description: `You have an <code>hires</code> table ordered by hire date. Show each employee's salary alongside the <strong>salary of the person hired just before them</strong> in the same department.\n\nReturn: <code>department</code>, <code>name</code>, <code>hire_date</code>, <code>salary</code>, <code>prev_salary</code>`,
    schema: `CREATE TABLE hires (emp_id INT, name TEXT, department TEXT, hire_date TEXT, salary INT);
INSERT INTO hires VALUES
(1,'Alice','Eng','2022-01-10',80000),(2,'Bob','Eng','2022-06-01',85000),(3,'Carol','Eng','2023-01-15',90000),
(4,'David','Sales','2022-03-01',60000),(5,'Eve','Sales','2022-09-01',65000),(6,'Frank','Sales','2023-02-01',70000);`,
    hint: "LAG(salary) OVER (PARTITION BY department ORDER BY hire_date)",
    solution: `SELECT department, name, hire_date, salary,
  LAG(salary) OVER (PARTITION BY department ORDER BY hire_date) AS prev_salary
FROM hires ORDER BY department, hire_date;`
  },
  {
    id: 25, pattern: 5, difficulty: "Hard",
    title: "Detect Price Direction (Up / Down / Same)",
    description: `You have a <code>stock_prices</code> table. For each row, compare current price to the previous day's price and label it <code>'Up'</code>, <code>'Down'</code>, or <code>'Same'</code>. Also show how many consecutive days the current direction has held.\n\nReturn: <code>price_date</code>, <code>price</code>, <code>direction</code>`,
    schema: `CREATE TABLE stock_prices (id INT, price_date TEXT, price DECIMAL);
INSERT INTO stock_prices VALUES
(1,'2024-01-01',100.0),(2,'2024-01-02',102.5),(3,'2024-01-03',105.0),
(4,'2024-01-04',103.0),(5,'2024-01-05',103.0),(6,'2024-01-06',107.0),(7,'2024-01-07',106.0);`,
    hint: "Use LAG to get previous price, then CASE WHEN price > prev_price THEN 'Up'...",
    solution: `WITH with_prev AS (
  SELECT price_date, price,
    LAG(price) OVER (ORDER BY price_date) AS prev_price
  FROM stock_prices
)
SELECT price_date, price,
  CASE 
    WHEN prev_price IS NULL THEN 'N/A'
    WHEN price > prev_price THEN 'Up'
    WHEN price < prev_price THEN 'Down'
    ELSE 'Same'
  END AS direction
FROM with_prev ORDER BY price_date;`
  },

  // ─── PATTERN 6: Day-1 Retention ───────────────────────────────────────────
  {
    id: 26, pattern: 6, difficulty: "Easy",
    title: "Day-1 User Retention",
    description: `You have a <code>logins</code> table. Find all users who logged in on <strong>day 1 (the day after signup)</strong>.\n\nReturn: <code>user_id</code>, <code>signup_date</code>, <code>retained</code> (1 if retained, 0 if not)`,
    schema: `CREATE TABLE user_signups (user_id INT, signup_date TEXT);
CREATE TABLE logins (user_id INT, login_date TEXT);
INSERT INTO user_signups VALUES (101,'2024-01-01'),(102,'2024-01-01'),(103,'2024-01-02'),(104,'2024-01-02');
INSERT INTO logins VALUES (101,'2024-01-02'),(101,'2024-01-05'),(102,'2024-01-05'),(103,'2024-01-03'),(104,'2024-01-04');`,
    hint: "JOIN logins ON login_date = signup_date + 1 day, or use EXISTS with date arithmetic",
    solution: `SELECT s.user_id, s.signup_date,
  CASE WHEN EXISTS (
    SELECT 1 FROM logins l 
    WHERE l.user_id = s.user_id 
    AND l.login_date = DATE_ADD(s.signup_date, INTERVAL 1 DAY)
  ) THEN 1 ELSE 0 END AS retained
FROM user_signups s;`
  },
  {
    id: 27, pattern: 6, difficulty: "Medium",
    title: "Day-1 Retention Rate by Signup Date",
    description: `Using the same <code>user_signups</code> and <code>logins</code> tables, calculate the <strong>Day-1 retention rate per signup date</strong>.\n\nReturn: <code>signup_date</code>, <code>total_users</code>, <code>retained_users</code>, <code>retention_rate</code>`,
    schema: `CREATE TABLE user_signups2 (user_id INT, signup_date TEXT);
CREATE TABLE logins2 (user_id INT, login_date TEXT);
INSERT INTO user_signups2 VALUES (101,'2024-01-01'),(102,'2024-01-01'),(103,'2024-01-01'),(104,'2024-01-02'),(105,'2024-01-02');
INSERT INTO logins2 VALUES (101,'2024-01-02'),(103,'2024-01-02'),(104,'2024-01-03');`,
    hint: "LEFT JOIN logins on next-day condition, then GROUP BY signup_date and COUNT",
    solution: `SELECT s.signup_date,
  COUNT(DISTINCT s.user_id) AS total_users,
  COUNT(DISTINCT l.user_id) AS retained_users,
  ROUND(COUNT(DISTINCT l.user_id) * 100.0 / COUNT(DISTINCT s.user_id), 2) AS retention_rate
FROM user_signups2 s
LEFT JOIN logins2 l ON l.user_id = s.user_id AND l.login_date = DATE_ADD(s.signup_date, INTERVAL 1 DAY)
GROUP BY s.signup_date ORDER BY s.signup_date;`
  },
  {
    id: 28, pattern: 6, difficulty: "Medium",
    title: "7-Day Retention Check",
    description: `You have <code>app_users</code> (signup) and <code>app_sessions</code> (activity). Find users who were active within <strong>7 days of signing up</strong>.\n\nReturn: <code>user_id</code>, <code>signup_date</code>, <code>day7_retained</code>`,
    schema: `CREATE TABLE app_users (user_id INT, signup_date TEXT);
CREATE TABLE app_sessions (user_id INT, session_date TEXT);
INSERT INTO app_users VALUES (1,'2024-01-01'),(2,'2024-01-01'),(3,'2024-01-01'),(4,'2024-01-02');
INSERT INTO app_sessions VALUES (1,'2024-01-05'),(1,'2024-01-08'),(2,'2024-01-09'),(3,'2024-01-15'),(4,'2024-01-08');`,
    hint: "JOIN on session_date BETWEEN signup_date+1 AND signup_date+7",
    solution: `SELECT u.user_id, u.signup_date,
  CASE WHEN EXISTS (
    SELECT 1 FROM app_sessions s 
    WHERE s.user_id = u.user_id
    AND s.session_date BETWEEN DATE_ADD(u.signup_date, INTERVAL 1 DAY) AND DATE_ADD(u.signup_date, INTERVAL 7 DAY)
  ) THEN 1 ELSE 0 END AS day7_retained
FROM app_users u;`
  },
  {
    id: 29, pattern: 6, difficulty: "Hard",
    title: "Retention by Signup Cohort (Day 1 and Day 7)",
    description: `You have <code>cohort_users</code> and <code>cohort_activity</code>. For each signup cohort (by week), compute both <strong>Day-1 and Day-7 retention rates</strong>.\n\nReturn: <code>signup_week</code>, <code>total</code>, <code>day1_retained</code>, <code>day7_retained</code>, <code>day1_rate</code>, <code>day7_rate</code>`,
    schema: `CREATE TABLE cohort_users (user_id INT, signup_date TEXT);
CREATE TABLE cohort_activity (user_id INT, activity_date TEXT);
INSERT INTO cohort_users VALUES (1,'2024-01-01'),(2,'2024-01-01'),(3,'2024-01-02'),(4,'2024-01-08'),(5,'2024-01-08');
INSERT INTO cohort_activity VALUES (1,'2024-01-02'),(1,'2024-01-08'),(2,'2024-01-09'),(3,'2024-01-03'),(4,'2024-01-09'),(4,'2024-01-15');`,
    hint: "Use conditional COUNT DISTINCT with date comparison for each retention milestone",
    solution: `SELECT 
  DATE_FORMAT(u.signup_date, '%Y-W%u') AS signup_week,
  COUNT(DISTINCT u.user_id) AS total,
  COUNT(DISTINCT CASE WHEN a1.user_id IS NOT NULL THEN u.user_id END) AS day1_retained,
  COUNT(DISTINCT CASE WHEN a7.user_id IS NOT NULL THEN u.user_id END) AS day7_retained,
  ROUND(COUNT(DISTINCT CASE WHEN a1.user_id IS NOT NULL THEN u.user_id END)*100.0/COUNT(DISTINCT u.user_id),2) AS day1_rate,
  ROUND(COUNT(DISTINCT CASE WHEN a7.user_id IS NOT NULL THEN u.user_id END)*100.0/COUNT(DISTINCT u.user_id),2) AS day7_rate
FROM cohort_users u
LEFT JOIN cohort_activity a1 ON a1.user_id=u.user_id AND a1.activity_date=DATE_ADD(u.signup_date, INTERVAL 1 DAY)
LEFT JOIN cohort_activity a7 ON a7.user_id=u.user_id AND a7.activity_date BETWEEN DATE_ADD(u.signup_date, INTERVAL 6 DAY) AND DATE_ADD(u.signup_date, INTERVAL 8 DAY)
GROUP BY signup_week;`
  },
  {
    id: 30, pattern: 6, difficulty: "Hard",
    title: "Same-Day Return Rate",
    description: `You have an <code>events</code> table. Find users who had <strong>more than one session on the same day</strong> (same-day return). Compute the same-day return rate per day.\n\nReturn: <code>event_date</code>, <code>total_users</code>, <code>returned_users</code>, <code>return_rate</code>`,
    schema: `CREATE TABLE events (event_id INT, user_id INT, event_date TEXT, session_num INT);
INSERT INTO events VALUES
(1,101,'2024-01-01',1),(2,101,'2024-01-01',2),(3,102,'2024-01-01',1),
(4,103,'2024-01-02',1),(5,103,'2024-01-02',2),(6,104,'2024-01-02',1),(7,105,'2024-01-02',1);`,
    hint: "GROUP BY date and user_id, count sessions per user per day, then aggregate by day",
    solution: `WITH user_sessions AS (
  SELECT event_date, user_id, COUNT(*) AS sessions
  FROM events GROUP BY event_date, user_id
)
SELECT event_date,
  COUNT(DISTINCT user_id) AS total_users,
  COUNT(DISTINCT CASE WHEN sessions > 1 THEN user_id END) AS returned_users,
  ROUND(COUNT(DISTINCT CASE WHEN sessions > 1 THEN user_id END)*100.0/COUNT(DISTINCT user_id),2) AS return_rate
FROM user_sessions GROUP BY event_date ORDER BY event_date;`
  },

  // ─── PATTERN 7: Sessionization ────────────────────────────────────────────
  {
    id: 31, pattern: 7, difficulty: "Medium",
    title: "Assign Session IDs (30-min Gap)",
    description: `You have a <code>page_events</code> table. Assign a <strong>session_id</strong> to each event — a new session starts when the gap from the previous event exceeds 30 minutes (1800 seconds).\n\nReturn: <code>user_id</code>, <code>event_time</code>, <code>page</code>, <code>session_id</code>`,
    schema: `CREATE TABLE page_events (event_id INT, user_id INT, event_time TEXT, page TEXT);
INSERT INTO page_events VALUES
(1,101,'2024-01-01 10:00:00','home'),(2,101,'2024-01-01 10:15:00','products'),
(3,101,'2024-01-01 10:45:00','cart'),(4,101,'2024-01-01 12:00:00','home'),
(5,101,'2024-01-01 12:10:00','checkout'),
(6,102,'2024-01-01 09:00:00','home'),(7,102,'2024-01-01 09:20:00','about');`,
    hint: "Use LAG to get previous event time, flag new session if gap > 1800s, then SUM the flag as running total",
    solution: `WITH gaps AS (
  SELECT *, 
    LAG(event_time) OVER (PARTITION BY user_id ORDER BY event_time) AS prev_time
  FROM page_events
),
flags AS (
  SELECT *,
    CASE WHEN prev_time IS NULL OR TIMESTAMPDIFF(SECOND, prev_time, event_time) > 1800 THEN 1 ELSE 0 END AS new_session
  FROM gaps
)
SELECT user_id, event_time, page,
  SUM(new_session) OVER (PARTITION BY user_id ORDER BY event_time) AS session_id
FROM flags ORDER BY user_id, event_time;`
  },
  {
    id: 32, pattern: 7, difficulty: "Medium",
    title: "Count Sessions per User",
    description: `Using the same logic as sessionization, count the <strong>total number of sessions per user</strong>.\n\nReturn: <code>user_id</code>, <code>session_count</code>`,
    schema: `CREATE TABLE clickstream (event_id INT, user_id INT, event_time TEXT);
INSERT INTO clickstream VALUES
(1,101,'2024-01-01 10:00:00'),(2,101,'2024-01-01 10:10:00'),(3,101,'2024-01-01 12:00:00'),
(4,101,'2024-01-01 12:05:00'),(5,101,'2024-01-01 15:00:00'),
(6,102,'2024-01-01 09:00:00'),(7,102,'2024-01-01 09:30:00'),(8,102,'2024-01-01 11:00:00');`,
    hint: "Sessionize first in a CTE, then SELECT user_id, MAX(session_id) from the result",
    solution: `WITH gaps AS (
  SELECT *, LAG(event_time) OVER (PARTITION BY user_id ORDER BY event_time) AS prev_time FROM clickstream
),
sessions AS (
  SELECT user_id, event_time,
    SUM(CASE WHEN prev_time IS NULL OR TIMESTAMPDIFF(SECOND, prev_time, event_time)>1800 THEN 1 ELSE 0 END)
      OVER (PARTITION BY user_id ORDER BY event_time) AS session_id
  FROM gaps
)
SELECT user_id, MAX(session_id) AS session_count FROM sessions GROUP BY user_id;`
  },
  {
    id: 33, pattern: 7, difficulty: "Hard",
    title: "Session Duration",
    description: `You have <code>sessions_raw</code> (already sessionized with session_id). Calculate the <strong>duration of each session in minutes</strong> as the difference between first and last event.\n\nReturn: <code>user_id</code>, <code>session_id</code>, <code>session_start</code>, <code>session_end</code>, <code>duration_mins</code>`,
    schema: `CREATE TABLE sessions_raw (event_id INT, user_id INT, session_id INT, event_time TEXT, page TEXT);
INSERT INTO sessions_raw VALUES
(1,101,1,'2024-01-01 10:00:00','home'),(2,101,1,'2024-01-01 10:15:00','products'),(3,101,1,'2024-01-01 10:25:00','cart'),
(4,101,2,'2024-01-01 12:00:00','home'),(5,101,2,'2024-01-01 12:08:00','checkout'),
(6,102,1,'2024-01-01 09:00:00','home'),(7,102,1,'2024-01-01 09:30:00','about');`,
    hint: "GROUP BY user_id, session_id and use MIN/MAX on event_time, then compute julianday difference",
    solution: `SELECT user_id, session_id,
  MIN(event_time) AS session_start,
  MAX(event_time) AS session_end,
  ROUND((julianday(MAX(event_time)) - julianday(MIN(event_time))) * 1440, 2) AS duration_mins
FROM sessions_raw GROUP BY user_id, session_id ORDER BY user_id, session_id;`
  },
  {
    id: 34, pattern: 7, difficulty: "Hard",
    title: "First Page per Session",
    description: `You have a sessionized <code>session_events</code> table. Find the <strong>first page visited in each session</strong> per user.\n\nReturn: <code>user_id</code>, <code>session_id</code>, <code>first_page</code>, <code>session_start</code>`,
    schema: `CREATE TABLE session_events (event_id INT, user_id INT, session_id INT, page TEXT, event_time TEXT);
INSERT INTO session_events VALUES
(1,101,1,'home','2024-01-01 10:00:00'),(2,101,1,'products','2024-01-01 10:05:00'),(3,101,1,'cart','2024-01-01 10:10:00'),
(4,101,2,'landing','2024-01-01 14:00:00'),(5,101,2,'pricing','2024-01-01 14:08:00'),
(6,102,1,'blog','2024-01-01 09:00:00'),(7,102,1,'home','2024-01-01 09:10:00');`,
    hint: "Use FIRST_VALUE(page) OVER (PARTITION BY user_id, session_id ORDER BY event_time) then deduplicate",
    solution: `SELECT DISTINCT user_id, session_id,
  FIRST_VALUE(page) OVER (PARTITION BY user_id, session_id ORDER BY event_time) AS first_page,
  FIRST_VALUE(event_time) OVER (PARTITION BY user_id, session_id ORDER BY event_time) AS session_start
FROM session_events ORDER BY user_id, session_id;`
  },
  {
    id: 35, pattern: 7, difficulty: "Hard",
    title: "Sessions per Day per User",
    description: `You have a <code>raw_events</code> table (not sessionized). Sessions are separated by 1-hour gaps. Find the <strong>number of sessions per user per day</strong>.\n\nReturn: <code>user_id</code>, <code>event_day</code>, <code>sessions_that_day</code>`,
    schema: `CREATE TABLE raw_events (id INT, user_id INT, event_time TEXT);
INSERT INTO raw_events VALUES
(1,101,'2024-01-01 08:00:00'),(2,101,'2024-01-01 08:30:00'),(3,101,'2024-01-01 10:00:00'),
(4,101,'2024-01-01 10:20:00'),(5,101,'2024-01-02 09:00:00'),
(6,102,'2024-01-01 11:00:00'),(7,102,'2024-01-01 13:30:00'),(8,102,'2024-01-01 13:50:00');`,
    hint: "Sessionize with 3600s threshold, then GROUP BY user_id, DATE(event_time) and COUNT DISTINCT session_id",
    solution: `WITH gaps AS (
  SELECT *, LAG(event_time) OVER (PARTITION BY user_id ORDER BY event_time) AS prev_time FROM raw_events
),
sess AS (
  SELECT user_id, event_time,
    SUM(CASE WHEN prev_time IS NULL OR TIMESTAMPDIFF(SECOND, prev_time, event_time)>3600 THEN 1 ELSE 0 END)
      OVER (PARTITION BY user_id ORDER BY event_time) AS session_id
  FROM gaps
)
SELECT user_id, DATE(event_time) AS event_day, MAX(session_id) AS sessions_that_day
FROM sess GROUP BY user_id, DATE(event_time) ORDER BY user_id, event_day;`
  },

  // ─── PATTERN 8: Consecutive Streaks ───────────────────────────────────────
  {
    id: 36, pattern: 8, difficulty: "Medium",
    title: "Login Streak Groups",
    description: `You have a <code>daily_logins</code> table. Find each user's <strong>consecutive login streak groups</strong> using the classic date - ROW_NUMBER trick.\n\nReturn: <code>user_id</code>, <code>login_date</code>, <code>streak_group</code>`,
    schema: `CREATE TABLE daily_logins (id INT, user_id INT, login_date TEXT);
INSERT INTO daily_logins VALUES
(1,101,'2024-01-01'),(2,101,'2024-01-02'),(3,101,'2024-01-03'),
(4,101,'2024-01-05'),(5,101,'2024-01-06'),
(6,102,'2024-01-01'),(7,102,'2024-01-03'),(8,102,'2024-01-04'),(9,102,'2024-01-05');`,
    hint: "DATE_SUB(login_date, INTERVAL (ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY login_date)) DAY) gives the streak group",
    solution: `SELECT user_id, login_date,
  DATE_SUB(login_date, INTERVAL ((ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY login_date) - 1)) DAY) AS streak_group
FROM daily_logins ORDER BY user_id, login_date;`
  },
  {
    id: 37, pattern: 8, difficulty: "Medium",
    title: "Longest Login Streak per User",
    description: `Using the streak grouping technique, find the <strong>longest consecutive login streak</strong> for each user.\n\nReturn: <code>user_id</code>, <code>streak_start</code>, <code>streak_end</code>, <code>streak_length</code>`,
    schema: `CREATE TABLE user_logins (id INT, user_id INT, login_date TEXT);
INSERT INTO user_logins VALUES
(1,101,'2024-01-01'),(2,101,'2024-01-02'),(3,101,'2024-01-03'),(4,101,'2024-01-05'),(5,101,'2024-01-06'),(6,101,'2024-01-07'),(7,101,'2024-01-08'),
(8,102,'2024-01-01'),(9,102,'2024-01-02'),(10,102,'2024-01-05');`,
    hint: "Use streak_group to GROUP BY, COUNT streak length, then pick the MAX per user",
    solution: `WITH groups AS (
  SELECT user_id, login_date,
    DATE_SUB(login_date, INTERVAL ((ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY login_date) - 1)) DAY) AS grp
  FROM user_logins
),
streaks AS (
  SELECT user_id, grp, MIN(login_date) AS streak_start, MAX(login_date) AS streak_end, COUNT(*) AS streak_length
  FROM groups GROUP BY user_id, grp
)
SELECT user_id, streak_start, streak_end, streak_length
FROM streaks WHERE streak_length = (SELECT MAX(s2.streak_length) FROM streaks s2 WHERE s2.user_id = streaks.user_id)
ORDER BY user_id;`
  },
  {
    id: 38, pattern: 8, difficulty: "Hard",
    title: "Current Active Streak",
    description: `You have <code>activity_log</code>. Find each user's <strong>current streak</strong> — the number of consecutive days up to and including the most recent activity date.\n\nReturn: <code>user_id</code>, <code>current_streak</code>`,
    schema: `CREATE TABLE activity_log (id INT, user_id INT, activity_date TEXT);
INSERT INTO activity_log VALUES
(1,101,'2024-01-10'),(2,101,'2024-01-11'),(3,101,'2024-01-12'),
(4,102,'2024-01-08'),(5,102,'2024-01-09'),(6,102,'2024-01-11'),(7,102,'2024-01-12'),
(8,103,'2024-01-05'),(9,103,'2024-01-12');`,
    hint: "Get streak groups, find the group that contains the user's MAX(activity_date), count its size",
    solution: `WITH groups AS (
  SELECT user_id, activity_date,
    DATE_SUB(activity_date, INTERVAL ((ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY activity_date)-1)) DAY) AS grp
  FROM activity_log
),
latest_grp AS (
  SELECT g.user_id, g.grp FROM groups g
  JOIN (SELECT user_id, MAX(activity_date) AS max_date FROM activity_log GROUP BY user_id) m
    ON g.user_id=m.user_id AND g.activity_date=m.max_date
)
SELECT g.user_id, COUNT(*) AS current_streak
FROM groups g JOIN latest_grp l ON g.user_id=l.user_id AND g.grp=l.grp
GROUP BY g.user_id ORDER BY g.user_id;`
  },
  {
    id: 39, pattern: 8, difficulty: "Hard",
    title: "Streak of Positive Sales Days",
    description: `You have a <code>sales_daily</code> table. Find the <strong>longest streak of consecutive days with revenue > 1000</strong> for each store.\n\nReturn: <code>store_id</code>, <code>max_streak</code>`,
    schema: `CREATE TABLE sales_daily (id INT, store_id INT, sale_date TEXT, revenue INT);
INSERT INTO sales_daily VALUES
(1,'S1','2024-01-01',1200),(2,'S1','2024-01-02',1100),(3,'S1','2024-01-03',900),(4,'S1','2024-01-04',1300),(5,'S1','2024-01-05',1400),(6,'S1','2024-01-06',1200),
(7,'S2','2024-01-01',800),(8,'S2','2024-01-02',1100),(9,'S2','2024-01-03',1200),(10,'S2','2024-01-04',1300);`,
    hint: "Filter to revenue > 1000 first, then apply streak logic on filtered rows",
    solution: `WITH good_days AS (
  SELECT store_id, sale_date FROM sales_daily WHERE revenue > 1000
),
groups AS (
  SELECT store_id, sale_date,
    DATE_SUB(sale_date, INTERVAL ((ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY sale_date)-1)) DAY) AS grp
  FROM good_days
)
SELECT store_id, MAX(cnt) AS max_streak FROM (
  SELECT store_id, grp, COUNT(*) AS cnt FROM groups GROUP BY store_id, grp
) GROUP BY store_id ORDER BY store_id;`
  },
  {
    id: 40, pattern: 8, difficulty: "Hard",
    title: "All Streaks with Start and End Dates",
    description: `You have a <code>checkins</code> table. Return <strong>all streaks</strong> (not just the longest) for each user, with start date, end date, and length.\n\nReturn: <code>user_id</code>, <code>streak_start</code>, <code>streak_end</code>, <code>streak_days</code>`,
    schema: `CREATE TABLE checkins (id INT, user_id INT, checkin_date TEXT);
INSERT INTO checkins VALUES
(1,101,'2024-01-01'),(2,101,'2024-01-02'),(3,101,'2024-01-04'),(4,101,'2024-01-05'),(5,101,'2024-01-06'),
(6,102,'2024-01-03'),(7,102,'2024-01-04'),(8,102,'2024-01-07'),(9,102,'2024-01-08'),(10,102,'2024-01-09'),(11,102,'2024-01-10');`,
    hint: "Use streak groups, then GROUP BY user_id, grp to get start, end, and count each streak",
    solution: `WITH grps AS (
  SELECT user_id, checkin_date,
    DATE_SUB(checkin_date, INTERVAL ((ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY checkin_date)-1)) DAY) AS grp
  FROM checkins
)
SELECT user_id, MIN(checkin_date) AS streak_start, MAX(checkin_date) AS streak_end, COUNT(*) AS streak_days
FROM grps GROUP BY user_id, grp ORDER BY user_id, streak_start;`
  },

  // ─── PATTERN 9: Gaps Between Events ───────────────────────────────────────
  {
    id: 41, pattern: 9, difficulty: "Easy",
    title: "Time Between Orders",
    description: `You have an <code>orders</code> table. For each order, show the <strong>number of days since the previous order</strong> for that customer.\n\nReturn: <code>customer_id</code>, <code>order_date</code>, <code>days_since_prev</code>`,
    schema: `CREATE TABLE gap_orders (order_id INT, customer_id INT, order_date TEXT);
INSERT INTO gap_orders VALUES
(1,101,'2024-01-05'),(2,101,'2024-01-12'),(3,101,'2024-01-20'),(4,101,'2024-02-01'),
(5,102,'2024-01-03'),(6,102,'2024-01-15'),(7,102,'2024-02-10');`,
    hint: "DATEDIFF(order_date, LAG(order_date) OVER (PARTITION BY customer_id ORDER BY order_date))",
    solution: `SELECT customer_id, order_date,
  ROUND(DATEDIFF(order_date, LAG(order_date) OVER (PARTITION BY customer_id ORDER BY order_date)), 0) AS days_since_prev
FROM gap_orders ORDER BY customer_id, order_date;`
  },
  {
    id: 42, pattern: 9, difficulty: "Medium",
    title: "Average Gap Between Logins",
    description: `You have a <code>login_log</code> table. Calculate the <strong>average gap in days between logins</strong> for each user.\n\nReturn: <code>user_id</code>, <code>avg_gap_days</code>`,
    schema: `CREATE TABLE login_log (id INT, user_id INT, login_date TEXT);
INSERT INTO login_log VALUES
(1,101,'2024-01-01'),(2,101,'2024-01-04'),(3,101,'2024-01-10'),(4,101,'2024-01-15'),
(5,102,'2024-01-02'),(6,102,'2024-01-03'),(7,102,'2024-01-08');`,
    hint: "Get gaps with LAG, then AVG the gaps per user (ignoring NULLs)",
    solution: `WITH gaps AS (
  SELECT user_id,
    DATEDIFF(login_date, LAG(login_date) OVER (PARTITION BY user_id ORDER BY login_date)) AS gap
  FROM login_log
)
SELECT user_id, ROUND(AVG(gap), 2) AS avg_gap_days FROM gaps WHERE gap IS NOT NULL GROUP BY user_id;`
  },
  {
    id: 43, pattern: 9, difficulty: "Medium",
    title: "Longest Gap Between Purchases",
    description: `You have a <code>purchases</code> table. Find the <strong>longest gap in days between consecutive purchases</strong> for each customer.\n\nReturn: <code>customer_id</code>, <code>max_gap_days</code>`,
    schema: `CREATE TABLE purchases (id INT, customer_id INT, purchase_date TEXT);
INSERT INTO purchases VALUES
(1,201,'2024-01-01'),(2,201,'2024-01-15'),(3,201,'2024-01-16'),(4,201,'2024-02-20'),
(5,202,'2024-01-05'),(6,202,'2024-01-07'),(7,202,'2024-02-15');`,
    hint: "Calculate all gaps, then MAX per customer",
    solution: `WITH gaps AS (
  SELECT customer_id,
    ROUND(DATEDIFF(purchase_date, LAG(purchase_date) OVER (PARTITION BY customer_id ORDER BY purchase_date)), 0) AS gap
  FROM purchases
)
SELECT customer_id, MAX(gap) AS max_gap_days FROM gaps WHERE gap IS NOT NULL GROUP BY customer_id;`
  },
  {
    id: 44, pattern: 9, difficulty: "Hard",
    title: "Find Periods of Inactivity > 30 Days",
    description: `You have a <code>user_events</code> table. Find all gaps where a user was <strong>inactive for more than 30 days</strong>.\n\nReturn: <code>user_id</code>, <code>gap_start</code>, <code>gap_end</code>, <code>gap_days</code>`,
    schema: `CREATE TABLE user_events (id INT, user_id INT, event_date TEXT);
INSERT INTO user_events VALUES
(1,101,'2024-01-01'),(2,101,'2024-01-10'),(3,101,'2024-02-20'),(4,101,'2024-02-22'),
(5,102,'2024-01-05'),(6,102,'2024-01-10'),(7,102,'2024-03-01');`,
    hint: "Use LAG to get previous event date, compute gap, filter gap > 30",
    solution: `WITH gaps AS (
  SELECT user_id, 
    LAG(event_date) OVER (PARTITION BY user_id ORDER BY event_date) AS gap_start,
    event_date AS gap_end,
    ROUND(DATEDIFF(event_date, LAG(event_date) OVER (PARTITION BY user_id ORDER BY event_date)), 0) AS gap_days
  FROM user_events
)
SELECT user_id, gap_start, gap_end, gap_days FROM gaps WHERE gap_days > 30 ORDER BY user_id, gap_start;`
  },
  {
    id: 45, pattern: 9, difficulty: "Hard",
    title: "Classify Customers by Recency (Gap-based)",
    description: `You have a <code>customer_orders</code> table. Classify each customer by the gap since their last order: <code>'Active'</code> (≤30 days), <code>'At Risk'</code> (31-90 days), <code>'Churned'</code> (>90 days). Use '2024-03-01' as today.\n\nReturn: <code>customer_id</code>, <code>last_order_date</code>, <code>days_since</code>, <code>status</code>`,
    schema: `CREATE TABLE customer_orders (id INT, customer_id INT, order_date TEXT);
INSERT INTO customer_orders VALUES
(1,301,'2024-02-20'),(2,302,'2024-01-10'),(3,303,'2023-11-15'),
(4,304,'2024-02-28'),(5,305,'2023-12-01');`,
    hint: "GROUP BY customer, get MAX(order_date), compute days from today, then CASE WHEN",
    solution: `SELECT customer_id, 
  MAX(order_date) AS last_order_date,
  ROUNDDATEDIFF('2024-03-01', MAX(order_date), 0) AS days_since,
  CASE 
    WHEN DATEDIFF('2024-03-01', MAX(order_date)) <= 30 THEN 'Active'
    WHEN DATEDIFF('2024-03-01', MAX(order_date)) <= 90 THEN 'At Risk'
    ELSE 'Churned'
  END AS status
FROM customer_orders GROUP BY customer_id ORDER BY days_since;`
  },

  // ─── PATTERN 10: First / Last Value ───────────────────────────────────────
  {
    id: 46, pattern: 10, difficulty: "Easy",
    title: "First Purchase per User",
    description: `You have a <code>purchases</code> table. Find the <strong>first purchase</strong> (amount and date) for each user.\n\nReturn: <code>user_id</code>, <code>first_purchase_date</code>, <code>first_amount</code>`,
    schema: `CREATE TABLE first_purchases (id INT, user_id INT, amount INT, purchase_date TEXT);
INSERT INTO first_purchases VALUES
(1,101,150,'2024-01-05'),(2,101,200,'2024-01-10'),(3,101,100,'2024-02-01'),
(4,102,300,'2024-01-08'),(5,102,250,'2024-01-20'),
(6,103,80,'2024-01-15');`,
    hint: "FIRST_VALUE(amount) OVER (PARTITION BY user_id ORDER BY purchase_date)",
    solution: `SELECT DISTINCT user_id,
  FIRST_VALUE(purchase_date) OVER (PARTITION BY user_id ORDER BY purchase_date) AS first_purchase_date,
  FIRST_VALUE(amount) OVER (PARTITION BY user_id ORDER BY purchase_date) AS first_amount
FROM first_purchases ORDER BY user_id;`
  },
  {
    id: 47, pattern: 10, difficulty: "Easy",
    title: "Opening and Closing Price per Day",
    description: `You have a <code>trades</code> table with trade times and prices. Find the <strong>opening (first) and closing (last) price per trading day</strong>.\n\nReturn: <code>trade_date</code>, <code>open_price</code>, <code>close_price</code>`,
    schema: `CREATE TABLE trades (trade_id INT, trade_date TEXT, trade_time TEXT, price DECIMAL);
INSERT INTO trades VALUES
(1,'2024-01-01','09:30',100.0),(2,'2024-01-01','11:00',102.5),(3,'2024-01-01','15:30',101.0),
(4,'2024-01-02','09:30',101.5),(5,'2024-01-02','12:00',103.0),(6,'2024-01-02','15:30',104.5);`,
    hint: "FIRST_VALUE(price) and LAST_VALUE(price) OVER (PARTITION BY trade_date ORDER BY trade_time ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)",
    solution: `SELECT DISTINCT trade_date,
  FIRST_VALUE(price) OVER (PARTITION BY trade_date ORDER BY trade_time) AS open_price,
  LAST_VALUE(price) OVER (PARTITION BY trade_date ORDER BY trade_time ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS close_price
FROM trades ORDER BY trade_date;`
  },
  {
    id: 48, pattern: 10, difficulty: "Medium",
    title: "First and Last Touchpoints in Customer Journey",
    description: `You have a <code>touchpoints</code> table. Find the <strong>first and last channel</strong> a customer interacted with before conversion.\n\nReturn: <code>customer_id</code>, <code>first_channel</code>, <code>last_channel</code>`,
    schema: `CREATE TABLE touchpoints (id INT, customer_id INT, channel TEXT, touch_time TEXT);
INSERT INTO touchpoints VALUES
(1,101,'organic_search','2024-01-01 10:00'),(2,101,'email','2024-01-03 14:00'),(3,101,'paid_ad','2024-01-05 16:00'),
(4,102,'social','2024-01-02 11:00'),(5,102,'direct','2024-01-04 09:00'),
(6,103,'paid_ad','2024-01-01 08:00'),(7,103,'organic_search','2024-01-06 15:00'),(8,103,'email','2024-01-07 10:00');`,
    hint: "Use FIRST_VALUE and LAST_VALUE with UNBOUNDED frame, then SELECT DISTINCT",
    solution: `SELECT DISTINCT customer_id,
  FIRST_VALUE(channel) OVER (PARTITION BY customer_id ORDER BY touch_time) AS first_channel,
  LAST_VALUE(channel) OVER (PARTITION BY customer_id ORDER BY touch_time ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_channel
FROM touchpoints ORDER BY customer_id;`
  },
  {
    id: 49, pattern: 10, difficulty: "Medium",
    title: "Most Recent Device Used per User",
    description: `You have a <code>device_usage</code> table. Show the <strong>most recent device</strong> each user used alongside their very first device.\n\nReturn: <code>user_id</code>, <code>first_device</code>, <code>latest_device</code>`,
    schema: `CREATE TABLE device_usage (id INT, user_id INT, device TEXT, used_at TEXT);
INSERT INTO device_usage VALUES
(1,101,'desktop','2024-01-01'),(2,101,'mobile','2024-01-05'),(3,101,'tablet','2024-01-10'),
(4,102,'mobile','2024-01-02'),(5,102,'mobile','2024-01-08'),(6,102,'desktop','2024-01-15'),
(7,103,'tablet','2024-01-03'),(8,103,'mobile','2024-01-12');`,
    hint: "FIRST_VALUE(device) ORDER BY used_at ASC = first device; FIRST_VALUE(device) ORDER BY used_at DESC = latest device",
    solution: `SELECT DISTINCT user_id,
  FIRST_VALUE(device) OVER (PARTITION BY user_id ORDER BY used_at ASC) AS first_device,
  FIRST_VALUE(device) OVER (PARTITION BY user_id ORDER BY used_at DESC) AS latest_device
FROM device_usage ORDER BY user_id;`
  },
  {
    id: 50, pattern: 10, difficulty: "Hard",
    title: "Price at Start vs End of Month",
    description: `You have a <code>daily_prices</code> table. Find the <strong>first and last price of each month</strong> per product, and the <strong>monthly price change</strong>.\n\nReturn: <code>product_id</code>, <code>month</code>, <code>start_price</code>, <code>end_price</code>, <code>change</code>`,
    schema: `CREATE TABLE daily_prices (id INT, product_id TEXT, price_date TEXT, price DECIMAL);
INSERT INTO daily_prices VALUES
(1,'P1','2024-01-02',100),(2,'P1','2024-01-15',105),(3,'P1','2024-01-30',110),
(4,'P1','2024-02-01',108),(5,'P1','2024-02-14',112),(6,'P1','2024-02-28',115),
(7,'P2','2024-01-03',50),(8,'P2','2024-01-20',48),(9,'P2','2024-02-05',52),(10,'P2','2024-02-25',55);`,
    hint: "Use FIRST_VALUE and LAST_VALUE partitioned by product_id and month, then SELECT DISTINCT",
    solution: `SELECT DISTINCT product_id,
  DATE_FORMAT(price_date, '%Y-%m') AS month,
  FIRST_VALUE(price) OVER (PARTITION BY product_id, DATE_FORMAT(price_date, '%Y-%m') ORDER BY price_date) AS start_price,
  LAST_VALUE(price) OVER (PARTITION BY product_id, DATE_FORMAT(price_date, '%Y-%m') ORDER BY price_date ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS end_price,
  LAST_VALUE(price) OVER (PARTITION BY product_id, DATE_FORMAT(price_date, '%Y-%m') ORDER BY price_date ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)
  - FIRST_VALUE(price) OVER (PARTITION BY product_id, DATE_FORMAT(price_date, '%Y-%m') ORDER BY price_date) AS change
FROM daily_prices ORDER BY product_id, month;`
  },

  // ─── PATTERN 11: Percent of Total ─────────────────────────────────────────
  {
    id: 51, pattern: 11, difficulty: "Easy",
    title: "Revenue Share per Product",
    description: `You have a <code>product_revenue</code> table. Calculate each product's <strong>revenue as a % of total revenue</strong>.\n\nReturn: <code>product_name</code>, <code>revenue</code>, <code>pct_of_total</code>`,
    schema: `CREATE TABLE product_revenue (id INT, product_name TEXT, revenue INT);
INSERT INTO product_revenue VALUES
(1,'Laptop',50000),(2,'Phone',30000),(3,'Tablet',15000),(4,'Watch',5000);`,
    hint: "revenue / SUM(revenue) OVER () * 100",
    solution: `SELECT product_name, revenue,
  ROUND(revenue * 100.0 / SUM(revenue) OVER (), 2) AS pct_of_total
FROM product_revenue ORDER BY revenue DESC;`
  },
  {
    id: 52, pattern: 11, difficulty: "Easy",
    title: "Department Headcount as % of Company",
    description: `You have an <code>dept_headcount</code> table. Show each department's headcount and its <strong>percentage of total company headcount</strong>.\n\nReturn: <code>department</code>, <code>headcount</code>, <code>pct_of_total</code>`,
    schema: `CREATE TABLE dept_headcount (id INT, department TEXT, headcount INT);
INSERT INTO dept_headcount VALUES
(1,'Engineering',50),(2,'Sales',30),(3,'Marketing',20),(4,'HR',10),(5,'Finance',15);`,
    hint: "headcount * 100.0 / SUM(headcount) OVER ()",
    solution: `SELECT department, headcount,
  ROUND(headcount * 100.0 / SUM(headcount) OVER (), 2) AS pct_of_total
FROM dept_headcount ORDER BY headcount DESC;`
  },
  {
    id: 53, pattern: 11, difficulty: "Medium",
    title: "Category Revenue % Within Each Region",
    description: `You have a <code>regional_sales</code> table. Show each category's revenue as a <strong>% of its region's total</strong>.\n\nReturn: <code>region</code>, <code>category</code>, <code>revenue</code>, <code>pct_of_region</code>`,
    schema: `CREATE TABLE regional_sales (id INT, region TEXT, category TEXT, revenue INT);
INSERT INTO regional_sales VALUES
(1,'North','Electronics',10000),(2,'North','Clothing',5000),(3,'North','Food',3000),
(4,'South','Electronics',8000),(5,'South','Clothing',6000),(6,'South','Food',4000);`,
    hint: "SUM(revenue) OVER (PARTITION BY region) gives region total for the denominator",
    solution: `SELECT region, category, revenue,
  ROUND(revenue * 100.0 / SUM(revenue) OVER (PARTITION BY region), 2) AS pct_of_region
FROM regional_sales ORDER BY region, revenue DESC;`
  },
  {
    id: 54, pattern: 11, difficulty: "Medium",
    title: "Each User's Spend as % of Their Cohort",
    description: `You have a <code>user_spend</code> table. Calculate each user's total spend as a <strong>% of their cohort's total spend</strong>.\n\nReturn: <code>user_id</code>, <code>cohort</code>, <code>total_spend</code>, <code>pct_of_cohort</code>`,
    schema: `CREATE TABLE user_spend (id INT, user_id INT, cohort TEXT, amount INT);
INSERT INTO user_spend VALUES
(1,101,'2024-Q1',500),(2,102,'2024-Q1',800),(3,103,'2024-Q1',300),
(4,104,'2024-Q2',600),(5,105,'2024-Q2',400),(6,106,'2024-Q2',1000);`,
    hint: "SUM(amount) OVER (PARTITION BY cohort) for cohort total",
    solution: `SELECT user_id, cohort, amount AS total_spend,
  ROUND(amount * 100.0 / SUM(amount) OVER (PARTITION BY cohort), 2) AS pct_of_cohort
FROM user_spend ORDER BY cohort, amount DESC;`
  },
  {
    id: 55, pattern: 11, difficulty: "Hard",
    title: "Monthly Revenue as % of Annual",
    description: `You have a <code>monthly_sales</code> table. Show each month's revenue as a <strong>% of that year's total revenue</strong>.\n\nReturn: <code>year</code>, <code>month</code>, <code>revenue</code>, <code>pct_of_year</code>`,
    schema: `CREATE TABLE monthly_sales (id INT, year INT, month INT, revenue INT);
INSERT INTO monthly_sales VALUES
(1,2024,1,10000),(2,2024,2,12000),(3,2024,3,11000),(4,2024,4,13000),
(5,2023,1,8000),(6,2023,2,9000),(7,2023,3,8500),(8,2023,4,10000);`,
    hint: "PARTITION BY year in the window function to scope the denominator to annual total",
    solution: `SELECT year, month, revenue,
  ROUND(revenue * 100.0 / SUM(revenue) OVER (PARTITION BY year), 2) AS pct_of_year
FROM monthly_sales ORDER BY year, month;`
  },

  // ─── PATTERN 12: Ranking Variants ─────────────────────────────────────────
  {
    id: 56, pattern: 12, difficulty: "Easy",
    title: "Rank vs Dense Rank on Tied Scores",
    description: `You have a <code>scores</code> table. Show RANK, DENSE_RANK, and ROW_NUMBER for all students — observe the difference when scores are tied.\n\nReturn: <code>student_name</code>, <code>score</code>, <code>row_num</code>, <code>rank_val</code>, <code>dense_rank_val</code>`,
    schema: `CREATE TABLE scores (id INT, student_name TEXT, score INT);
INSERT INTO scores VALUES
(1,'Alice',95),(2,'Bob',88),(3,'Carol',95),(4,'David',88),(5,'Eve',92),(6,'Frank',85);`,
    hint: "Apply all three ranking functions with the same ORDER BY score DESC",
    solution: `SELECT student_name, score,
  ROW_NUMBER() OVER (ORDER BY score DESC) AS row_num,
  RANK() OVER (ORDER BY score DESC) AS rank_val,
  DENSE_RANK() OVER (ORDER BY score DESC) AS dense_rank_val
FROM scores ORDER BY score DESC;`
  },
  {
    id: 57, pattern: 12, difficulty: "Easy",
    title: "Top 5 Products by Sales (Handle Ties with DENSE_RANK)",
    description: `You have a <code>product_sales_rank</code> table. Return the <strong>top 5 products by sales using DENSE_RANK</strong> so tied products all appear.\n\nReturn: <code>product_name</code>, <code>sales</code>, <code>sales_rank</code>`,
    schema: `CREATE TABLE product_sales_rank (id INT, product_name TEXT, sales INT);
INSERT INTO product_sales_rank VALUES
(1,'A',5000),(2,'B',4500),(3,'C',5000),(4,'D',4000),(5,'E',4500),(6,'F',3500),(7,'G',4000),(8,'H',3000);`,
    hint: "DENSE_RANK() OVER (ORDER BY sales DESC), then filter WHERE dense_rank <= 5",
    solution: `SELECT product_name, sales, dense_rank_val FROM (
  SELECT product_name, sales,
    DENSE_RANK() OVER (ORDER BY sales DESC) AS dense_rank_val
  FROM product_sales_rank
) WHERE dense_rank_val <= 5 ORDER BY sales DESC;`
  },
  {
    id: 58, pattern: 12, difficulty: "Medium",
    title: "Percentile Rank of Each Employee's Salary",
    description: `You have an <code>emp_salaries</code> table. Calculate the <strong>percentile rank</strong> of each employee's salary using PERCENT_RANK or manual calculation.\n\nReturn: <code>name</code>, <code>salary</code>, <code>percentile_rank</code>`,
    schema: `CREATE TABLE emp_salaries (id INT, name TEXT, salary INT);
INSERT INTO emp_salaries VALUES
(1,'Alice',90000),(2,'Bob',75000),(3,'Carol',85000),(4,'David',60000),
(5,'Eve',95000),(6,'Frank',70000),(7,'Grace',80000);`,
    hint: "(RANK() - 1) / (COUNT(*) - 1) gives percentile rank manually",
    solution: `SELECT name, salary,
  ROUND((RANK() OVER (ORDER BY salary ASC) - 1) * 1.0 / (COUNT(*) OVER () - 1), 4) AS percentile_rank
FROM emp_salaries ORDER BY salary;`
  },
  {
    id: 59, pattern: 12, difficulty: "Medium",
    title: "Rank Products Within Category (Dense Rank)",
    description: `You have a <code>catalog</code> table. Rank products within each category using <strong>DENSE_RANK</strong> so no rank numbers are skipped.\n\nReturn: <code>category</code>, <code>product_name</code>, <code>price</code>, <code>price_rank</code>`,
    schema: `CREATE TABLE catalog (id INT, category TEXT, product_name TEXT, price DECIMAL);
INSERT INTO catalog VALUES
(1,'Electronics','Laptop',999),(2,'Electronics','Phone',699),(3,'Electronics','Tablet',499),
(4,'Electronics','Watch',299),(5,'Clothing','Jacket',150),(6,'Clothing','Shirt',50),
(7,'Clothing','Pants',80),(8,'Clothing','Shoes',120);`,
    hint: "DENSE_RANK() OVER (PARTITION BY category ORDER BY price DESC)",
    solution: `SELECT category, product_name, price,
  DENSE_RANK() OVER (PARTITION BY category ORDER BY price DESC) AS price_rank
FROM catalog ORDER BY category, price_rank;`
  },
  {
    id: 60, pattern: 12, difficulty: "Hard",
    title: "Rank Stores by Revenue — Flag Top 25%",
    description: `You have a <code>store_revenue</code> table. Rank all stores by revenue and flag those in the <strong>top 25% as 'Elite'</strong>, else 'Standard'.\n\nReturn: <code>store_id</code>, <code>revenue</code>, <code>revenue_rank</code>, <code>tier</code>`,
    schema: `CREATE TABLE store_revenue (store_id TEXT, revenue INT);
INSERT INTO store_revenue VALUES
('S1',95000),('S2',80000),('S3',70000),('S4',60000),('S5',50000),
('S6',45000),('S7',40000),('S8',35000);`,
    hint: "Use NTILE(4) OVER (ORDER BY revenue DESC) — stores in bucket 1 are top 25%",
    solution: `SELECT store_id, revenue,
  RANK() OVER (ORDER BY revenue DESC) AS revenue_rank,
  CASE WHEN NTILE(4) OVER (ORDER BY revenue DESC) = 1 THEN 'Elite' ELSE 'Standard' END AS tier
FROM store_revenue ORDER BY revenue DESC;`
  },

  // ─── PATTERN 13: Conditional Aggregation ──────────────────────────────────
  {
    id: 61, pattern: 13, difficulty: "Easy",
    title: "Count Orders by Status",
    description: `You have an <code>all_orders</code> table. In a single query, count the number of orders in each status: <strong>pending, shipped, delivered, cancelled</strong> — all in one row.\n\nReturn: <code>pending</code>, <code>shipped</code>, <code>delivered</code>, <code>cancelled</code>`,
    schema: `CREATE TABLE all_orders (order_id INT, status TEXT);
INSERT INTO all_orders VALUES
(1,'pending'),(2,'shipped'),(3,'delivered'),(4,'pending'),(5,'shipped'),
(6,'cancelled'),(7,'delivered'),(8,'delivered'),(9,'pending'),(10,'shipped');`,
    hint: "SUM(CASE WHEN status = 'X' THEN 1 ELSE 0 END) for each status",
    solution: `SELECT
  SUM(CASE WHEN status='pending' THEN 1 ELSE 0 END) AS pending,
  SUM(CASE WHEN status='shipped' THEN 1 ELSE 0 END) AS shipped,
  SUM(CASE WHEN status='delivered' THEN 1 ELSE 0 END) AS delivered,
  SUM(CASE WHEN status='cancelled' THEN 1 ELSE 0 END) AS cancelled
FROM all_orders;`
  },
  {
    id: 62, pattern: 13, difficulty: "Easy",
    title: "Revenue Split by Device Type",
    description: `You have a <code>device_orders</code> table. Calculate total revenue from <strong>mobile vs desktop</strong> side by side, per month.\n\nReturn: <code>month</code>, <code>mobile_revenue</code>, <code>desktop_revenue</code>`,
    schema: `CREATE TABLE device_orders (id INT, month TEXT, device TEXT, revenue INT);
INSERT INTO device_orders VALUES
(1,'2024-01','mobile',500),(2,'2024-01','desktop',800),(3,'2024-01','mobile',300),
(4,'2024-02','mobile',600),(5,'2024-02','desktop',700),(6,'2024-02','desktop',400),(7,'2024-02','mobile',200);`,
    hint: "SUM(CASE WHEN device='mobile' THEN revenue END) and SUM(CASE WHEN device='desktop' THEN revenue END)",
    solution: `SELECT month,
  SUM(CASE WHEN device='mobile' THEN revenue ELSE 0 END) AS mobile_revenue,
  SUM(CASE WHEN device='desktop' THEN revenue ELSE 0 END) AS desktop_revenue
FROM device_orders GROUP BY month ORDER BY month;`
  },
  {
    id: 63, pattern: 13, difficulty: "Medium",
    title: "Pass / Fail Rate per Subject",
    description: `You have a <code>exam_results</code> table (pass threshold: score >= 60). Calculate <strong>pass rate and fail rate per subject</strong>.\n\nReturn: <code>subject</code>, <code>total</code>, <code>passed</code>, <code>failed</code>, <code>pass_rate</code>`,
    schema: `CREATE TABLE exam_results (id INT, student TEXT, subject TEXT, score INT);
INSERT INTO exam_results VALUES
(1,'A','Math',75),(2,'B','Math',55),(3,'C','Math',80),(4,'D','Math',45),(5,'E','Math',90),
(6,'A','Science',65),(7,'B','Science',40),(8,'C','Science',70),(9,'D','Science',85),(10,'E','Science',55);`,
    hint: "SUM(CASE WHEN score >= 60 THEN 1 ELSE 0 END) for pass count",
    solution: `SELECT subject,
  COUNT(*) AS total,
  SUM(CASE WHEN score >= 60 THEN 1 ELSE 0 END) AS passed,
  SUM(CASE WHEN score < 60 THEN 1 ELSE 0 END) AS failed,
  ROUND(SUM(CASE WHEN score >= 60 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS pass_rate
FROM exam_results GROUP BY subject ORDER BY subject;`
  },
  {
    id: 64, pattern: 13, difficulty: "Medium",
    title: "Active vs Inactive Users per Month",
    description: `You have a <code>user_activity</code> table. Count <strong>active users (3+ activities) vs inactive users (< 3) per month</strong>.\n\nReturn: <code>month</code>, <code>active_users</code>, <code>inactive_users</code>`,
    schema: `CREATE TABLE user_activity (id INT, user_id INT, month TEXT, activity_count INT);
INSERT INTO user_activity VALUES
(1,101,'2024-01',5),(2,102,'2024-01',2),(3,103,'2024-01',8),(4,104,'2024-01',1),
(5,101,'2024-02',3),(6,102,'2024-02',4),(7,103,'2024-02',1),(8,104,'2024-02',6);`,
    hint: "SUM(CASE WHEN activity_count >= 3 THEN 1 ELSE 0 END) for active",
    solution: `SELECT month,
  SUM(CASE WHEN activity_count >= 3 THEN 1 ELSE 0 END) AS active_users,
  SUM(CASE WHEN activity_count < 3 THEN 1 ELSE 0 END) AS inactive_users
FROM user_activity GROUP BY month ORDER BY month;`
  },
  {
    id: 65, pattern: 13, difficulty: "Hard",
    title: "Revenue by Quarter (Pivot)",
    description: `You have a <code>quarterly_revenue</code> table. Pivot the data to show each year with <strong>Q1, Q2, Q3, Q4 revenue as separate columns</strong>.\n\nReturn: <code>year</code>, <code>q1</code>, <code>q2</code>, <code>q3</code>, <code>q4</code>`,
    schema: `CREATE TABLE quarterly_revenue (id INT, year INT, quarter TEXT, revenue INT);
INSERT INTO quarterly_revenue VALUES
(1,2023,'Q1',10000),(2,2023,'Q2',12000),(3,2023,'Q3',11000),(4,2023,'Q4',15000),
(5,2024,'Q1',13000),(6,2024,'Q2',14500),(7,2024,'Q3',16000),(8,2024,'Q4',18000);`,
    hint: "SUM(CASE WHEN quarter='Q1' THEN revenue END) etc., GROUP BY year",
    solution: `SELECT year,
  SUM(CASE WHEN quarter='Q1' THEN revenue END) AS q1,
  SUM(CASE WHEN quarter='Q2' THEN revenue END) AS q2,
  SUM(CASE WHEN quarter='Q3' THEN revenue END) AS q3,
  SUM(CASE WHEN quarter='Q4' THEN revenue END) AS q4
FROM quarterly_revenue GROUP BY year ORDER BY year;`
  },

  // ─── PATTERN 14: Funnel Analysis ──────────────────────────────────────────
  {
    id: 66, pattern: 14, difficulty: "Easy",
    title: "Basic 3-Step Conversion Funnel",
    description: `You have a <code>funnel_events</code> table with steps: <code>view</code>, <code>add_to_cart</code>, <code>purchase</code>. Count unique users at each step.\n\nReturn: <code>step</code>, <code>users</code>`,
    schema: `CREATE TABLE funnel_events (id INT, user_id INT, step TEXT, event_date TEXT);
INSERT INTO funnel_events VALUES
(1,101,'view','2024-01-01'),(2,102,'view','2024-01-01'),(3,103,'view','2024-01-01'),
(4,104,'view','2024-01-01'),(5,105,'view','2024-01-01'),
(6,101,'add_to_cart','2024-01-01'),(7,102,'add_to_cart','2024-01-01'),(8,103,'add_to_cart','2024-01-01'),
(9,101,'purchase','2024-01-01'),(10,102,'purchase','2024-01-01');`,
    hint: "COUNT(DISTINCT CASE WHEN step = 'view' THEN user_id END) for each step",
    solution: `SELECT
  COUNT(DISTINCT CASE WHEN step='view' THEN user_id END) AS view_users,
  COUNT(DISTINCT CASE WHEN step='add_to_cart' THEN user_id END) AS cart_users,
  COUNT(DISTINCT CASE WHEN step='purchase' THEN user_id END) AS purchase_users
FROM funnel_events;`
  },
  {
    id: 67, pattern: 14, difficulty: "Medium",
    title: "Funnel with Drop-off Rates",
    description: `Using the same funnel logic, calculate the <strong>drop-off rate between each step</strong>.\n\nReturn: <code>view_users</code>, <code>cart_users</code>, <code>purchase_users</code>, <code>view_to_cart_pct</code>, <code>cart_to_purchase_pct</code>`,
    schema: `CREATE TABLE funnel2 (id INT, user_id INT, step TEXT);
INSERT INTO funnel2 VALUES
(1,101,'view'),(2,102,'view'),(3,103,'view'),(4,104,'view'),(5,105,'view'),(6,106,'view'),(7,107,'view'),(8,108,'view'),(9,109,'view'),(10,110,'view'),
(11,101,'add_to_cart'),(12,102,'add_to_cart'),(13,103,'add_to_cart'),(14,104,'add_to_cart'),(15,105,'add_to_cart'),(16,106,'add_to_cart'),
(17,101,'purchase'),(18,102,'purchase'),(19,103,'purchase');`,
    hint: "Count each step, then divide cart/view and purchase/cart for rates",
    solution: `WITH counts AS (
  SELECT
    COUNT(DISTINCT CASE WHEN step='view' THEN user_id END) AS views,
    COUNT(DISTINCT CASE WHEN step='add_to_cart' THEN user_id END) AS carts,
    COUNT(DISTINCT CASE WHEN step='purchase' THEN user_id END) AS purchases
  FROM funnel2
)
SELECT views AS view_users, carts AS cart_users, purchases AS purchase_users,
  ROUND(carts*100.0/views,2) AS view_to_cart_pct,
  ROUND(purchases*100.0/carts,2) AS cart_to_purchase_pct
FROM counts;`
  },
  {
    id: 68, pattern: 14, difficulty: "Medium",
    title: "Funnel by Channel",
    description: `You have an <code>attributed_funnel</code> table (each user has an acquisition channel). Show the <strong>funnel completion rate per channel</strong>.\n\nReturn: <code>channel</code>, <code>top_of_funnel</code>, <code>converted</code>, <code>conversion_rate</code>`,
    schema: `CREATE TABLE attributed_funnel (id INT, user_id INT, channel TEXT, step TEXT);
INSERT INTO attributed_funnel VALUES
(1,101,'organic','view'),(2,101,'organic','purchase'),
(3,102,'paid','view'),(4,103,'paid','view'),(5,103,'paid','purchase'),
(6,104,'organic','view'),(7,105,'organic','view'),(8,105,'organic','purchase'),
(9,106,'paid','view'),(10,107,'paid','view'),(11,108,'social','view'),(12,108,'social','purchase');`,
    hint: "COUNT DISTINCT users at 'view' step and 'purchase' step, GROUP BY channel",
    solution: `SELECT channel,
  COUNT(DISTINCT CASE WHEN step='view' THEN user_id END) AS top_of_funnel,
  COUNT(DISTINCT CASE WHEN step='purchase' THEN user_id END) AS converted,
  ROUND(COUNT(DISTINCT CASE WHEN step='purchase' THEN user_id END)*100.0 / 
        COUNT(DISTINCT CASE WHEN step='view' THEN user_id END), 2) AS conversion_rate
FROM attributed_funnel GROUP BY channel ORDER BY conversion_rate DESC;`
  },
  {
    id: 69, pattern: 14, difficulty: "Hard",
    title: "Ordered Funnel (Must Complete Steps in Order)",
    description: `You have a <code>strict_funnel</code> table. Count users who completed steps <strong>in the correct order</strong>: must view BEFORE adding to cart, must add to cart BEFORE purchase.\n\nReturn: <code>viewed</code>, <code>carted_after_view</code>, <code>purchased_after_cart</code>`,
    schema: `CREATE TABLE strict_funnel (id INT, user_id INT, step TEXT, step_time TEXT);
INSERT INTO strict_funnel VALUES
(1,101,'view','2024-01-01 10:00'),(2,101,'add_to_cart','2024-01-01 10:05'),(3,101,'purchase','2024-01-01 10:10'),
(4,102,'add_to_cart','2024-01-01 09:00'),(5,102,'purchase','2024-01-01 09:05'),
(6,103,'view','2024-01-01 11:00'),(7,103,'add_to_cart','2024-01-01 11:10'),
(8,104,'view','2024-01-01 12:00'),(9,104,'purchase','2024-01-01 11:00');`,
    hint: "Use MIN(step_time) per user per step, then check if time ordering is correct",
    solution: `WITH step_times AS (
  SELECT user_id,
    MIN(CASE WHEN step='view' THEN step_time END) AS view_time,
    MIN(CASE WHEN step='add_to_cart' THEN step_time END) AS cart_time,
    MIN(CASE WHEN step='purchase' THEN step_time END) AS purchase_time
  FROM strict_funnel GROUP BY user_id
)
SELECT
  COUNT(DISTINCT CASE WHEN view_time IS NOT NULL THEN user_id END) AS viewed,
  COUNT(DISTINCT CASE WHEN cart_time > view_time THEN user_id END) AS carted_after_view,
  COUNT(DISTINCT CASE WHEN purchase_time > cart_time AND cart_time > view_time THEN user_id END) AS purchased_after_cart
FROM step_times;`
  },
  {
    id: 70, pattern: 14, difficulty: "Hard",
    title: "Weekly Funnel Comparison",
    description: `You have a <code>weekly_funnel</code> table. Compare the <strong>conversion rate (view → purchase) across weeks</strong>.\n\nReturn: <code>week</code>, <code>view_users</code>, <code>purchase_users</code>, <code>conversion_rate</code>`,
    schema: `CREATE TABLE weekly_funnel (id INT, user_id INT, step TEXT, event_date TEXT);
INSERT INTO weekly_funnel VALUES
(1,101,'view','2024-01-01'),(2,102,'view','2024-01-01'),(3,101,'purchase','2024-01-02'),
(4,103,'view','2024-01-08'),(5,104,'view','2024-01-08'),(6,105,'view','2024-01-08'),
(7,103,'purchase','2024-01-09'),(8,104,'purchase','2024-01-10');`,
    hint: "Extract week from event_date, GROUP BY week, count view users and purchase users",
    solution: `SELECT DATE_FORMAT(event_date, '%Y-W%u') AS week,
  COUNT(DISTINCT CASE WHEN step='view' THEN user_id END) AS view_users,
  COUNT(DISTINCT CASE WHEN step='purchase' THEN user_id END) AS purchase_users,
  ROUND(COUNT(DISTINCT CASE WHEN step='purchase' THEN user_id END)*100.0/
        COUNT(DISTINCT CASE WHEN step='view' THEN user_id END),2) AS conversion_rate
FROM weekly_funnel GROUP BY week ORDER BY week;`
  },

  // ─── PATTERN 15: Self Join for Pairs ──────────────────────────────────────
  {
    id: 71, pattern: 15, difficulty: "Medium",
    title: "Products Bought Together",
    description: `You have an <code>order_items</code> table. Find all <strong>pairs of products bought in the same order</strong>.\n\nReturn: <code>product_a</code>, <code>product_b</code>, <code>times_together</code>`,
    schema: `CREATE TABLE order_items (id INT, order_id INT, product TEXT);
INSERT INTO order_items VALUES
(1,1,'Apple'),(2,1,'Banana'),(3,1,'Cherry'),
(4,2,'Apple'),(5,2,'Banana'),
(6,3,'Banana'),(7,3,'Cherry'),
(8,4,'Apple'),(9,4,'Cherry');`,
    hint: "Self join on order_id with a.product < b.product to avoid duplicates, then GROUP BY and COUNT",
    solution: `SELECT a.product AS product_a, b.product AS product_b, COUNT(*) AS times_together
FROM order_items a JOIN order_items b ON a.order_id=b.order_id AND a.product < b.product
GROUP BY a.product, b.product ORDER BY times_together DESC;`
  },
  {
    id: 72, pattern: 15, difficulty: "Medium",
    title: "Employee-Manager Salary Comparison",
    description: `You have an <code>employees_mgr</code> table with a <code>manager_id</code> column. Find employees who earn <strong>more than their manager</strong>.\n\nReturn: <code>employee_name</code>, <code>employee_salary</code>, <code>manager_name</code>, <code>manager_salary</code>`,
    schema: `CREATE TABLE employees_mgr (emp_id INT, name TEXT, salary INT, manager_id INT);
INSERT INTO employees_mgr VALUES
(1,'CEO',200000,NULL),(2,'VP Eng',150000,1),(3,'VP Sales',130000,1),
(4,'Senior Dev',160000,2),(5,'Dev',90000,2),(6,'Sales Lead',140000,3),(7,'Sales Rep',70000,3);`,
    hint: "Self join employees on emp_id = manager_id, filter WHERE employee.salary > manager.salary",
    solution: `SELECT e.name AS employee_name, e.salary AS employee_salary,
  m.name AS manager_name, m.salary AS manager_salary
FROM employees_mgr e JOIN employees_mgr m ON e.manager_id = m.emp_id
WHERE e.salary > m.salary ORDER BY e.salary DESC;`
  },
  {
    id: 73, pattern: 15, difficulty: "Medium",
    title: "Friend Pairs (Mutual Follows)",
    description: `You have a <code>follows</code> table. Find all <strong>mutual follow pairs</strong> (user A follows B AND B follows A). Return each pair once.\n\nReturn: <code>user_a</code>, <code>user_b</code>`,
    schema: `CREATE TABLE follows (follower_id INT, following_id INT);
INSERT INTO follows VALUES
(1,2),(2,1),(1,3),(3,1),(2,3),(4,2),(5,1),(1,5);`,
    hint: "Self join where a.follower=b.following AND b.follower=a.following AND a.follower < b.follower",
    solution: `SELECT a.follower_id AS user_a, a.following_id AS user_b
FROM follows a JOIN follows b ON a.follower_id=b.following_id AND a.following_id=b.follower_id
WHERE a.follower_id < a.following_id ORDER BY user_a, user_b;`
  },
  {
    id: 74, pattern: 15, difficulty: "Hard",
    title: "Users Who Bought the Same Products",
    description: `You have a <code>user_purchases</code> table. Find pairs of users who have <strong>bought at least 2 of the same products</strong>.\n\nReturn: <code>user_a</code>, <code>user_b</code>, <code>common_products</code>`,
    schema: `CREATE TABLE user_purchases (id INT, user_id INT, product TEXT);
INSERT INTO user_purchases VALUES
(1,101,'Laptop'),(2,101,'Phone'),(3,101,'Tablet'),
(4,102,'Laptop'),(5,102,'Phone'),(6,102,'Watch'),
(7,103,'Tablet'),(8,103,'Laptop'),(9,103,'Phone'),
(10,104,'Watch'),(11,104,'Headphones');`,
    hint: "Self join on product, group by user pair, HAVING count >= 2",
    solution: `SELECT a.user_id AS user_a, b.user_id AS user_b, COUNT(*) AS common_products
FROM user_purchases a JOIN user_purchases b ON a.product=b.product AND a.user_id < b.user_id
GROUP BY a.user_id, b.user_id HAVING COUNT(*) >= 2 ORDER BY common_products DESC;`
  },
  {
    id: 75, pattern: 15, difficulty: "Hard",
    title: "Detect Duplicate Transactions",
    description: `You have a <code>bank_transactions</code> table. Find <strong>suspicious duplicate transactions</strong> — same user, same amount, within 1 minute of each other.\n\nReturn: <code>txn_id_a</code>, <code>txn_id_b</code>, <code>user_id</code>, <code>amount</code>`,
    schema: `CREATE TABLE bank_transactions (txn_id INT, user_id INT, amount DECIMAL, txn_time TEXT);
INSERT INTO bank_transactions VALUES
(1,101,50.00,'2024-01-01 10:00:00'),(2,101,50.00,'2024-01-01 10:00:30'),
(3,101,50.00,'2024-01-01 10:05:00'),(4,102,100.00,'2024-01-01 11:00:00'),
(5,102,100.00,'2024-01-01 11:00:45'),(6,103,75.00,'2024-01-01 12:00:00');`,
    hint: "Self join on user_id and amount, filter where time diff < 60 seconds and a.txn_id < b.txn_id",
    solution: `SELECT a.txn_id AS txn_id_a, b.txn_id AS txn_id_b, a.user_id, a.amount
FROM bank_transactions a JOIN bank_transactions b
  ON a.user_id=b.user_id AND a.amount=b.amount AND a.txn_id < b.txn_id
WHERE ABS(TIMESTAMPDIFF(SECOND, a.txn_time, b.txn_time)) < 60
ORDER BY a.user_id, a.txn_id;`
  },

  // ─── PATTERN 16: Anti Join (NOT EXISTS) ───────────────────────────────────
  {
    id: 76, pattern: 16, difficulty: "Easy",
    title: "Customers Who Never Purchased",
    description: `You have a <code>customers</code> and <code>orders</code> table. Find <strong>customers who have never placed an order</strong>.\n\nReturn: <code>customer_id</code>, <code>name</code>`,
    schema: `CREATE TABLE customers (customer_id INT, name TEXT);
CREATE TABLE ord_table (order_id INT, customer_id INT, amount INT);
INSERT INTO customers VALUES (1,'Alice'),(2,'Bob'),(3,'Carol'),(4,'David'),(5,'Eve');
INSERT INTO ord_table VALUES (1,1,100),(2,1,200),(3,3,150),(4,5,80);`,
    hint: "LEFT JOIN and WHERE order is NULL, or use NOT EXISTS / NOT IN",
    solution: `SELECT customer_id, name FROM customers
WHERE NOT EXISTS (
  SELECT 1 FROM ord_table o WHERE o.customer_id = customers.customer_id
);`
  },
  {
    id: 77, pattern: 16, difficulty: "Easy",
    title: "Products Never Ordered",
    description: `You have a <code>all_products</code> and <code>order_lines</code> table. Find <strong>products that have never been ordered</strong>.\n\nReturn: <code>product_id</code>, <code>product_name</code>`,
    schema: `CREATE TABLE all_products (product_id INT, product_name TEXT);
CREATE TABLE order_lines (line_id INT, product_id INT, quantity INT);
INSERT INTO all_products VALUES (1,'Widget A'),(2,'Widget B'),(3,'Widget C'),(4,'Widget D');
INSERT INTO order_lines VALUES (1,1,5),(2,2,3),(3,1,2);`,
    hint: "NOT EXISTS or LEFT JOIN...WHERE order_lines.product_id IS NULL",
    solution: `SELECT product_id, product_name FROM all_products
WHERE NOT EXISTS (
  SELECT 1 FROM order_lines ol WHERE ol.product_id = all_products.product_id
);`
  },
  {
    id: 78, pattern: 16, difficulty: "Medium",
    title: "Users Who Didn't Complete Onboarding",
    description: `You have a <code>registered_users</code> and an <code>onboarding_events</code> table. Find users who <strong>never completed the 'profile_complete' event</strong>.\n\nReturn: <code>user_id</code>, <code>email</code>`,
    schema: `CREATE TABLE registered_users (user_id INT, email TEXT, signup_date TEXT);
CREATE TABLE onboarding_events (id INT, user_id INT, event_type TEXT, event_date TEXT);
INSERT INTO registered_users VALUES (1,'a@test.com','2024-01-01'),(2,'b@test.com','2024-01-02'),(3,'c@test.com','2024-01-03'),(4,'d@test.com','2024-01-04');
INSERT INTO onboarding_events VALUES (1,1,'profile_complete','2024-01-01'),(2,1,'payment_added','2024-01-02'),(3,3,'profile_complete','2024-01-03'),(4,2,'payment_added','2024-01-03');`,
    hint: "NOT EXISTS with WHERE event_type = 'profile_complete'",
    solution: `SELECT user_id, email FROM registered_users r
WHERE NOT EXISTS (
  SELECT 1 FROM onboarding_events o 
  WHERE o.user_id = r.user_id AND o.event_type = 'profile_complete'
);`
  },
  {
    id: 79, pattern: 16, difficulty: "Medium",
    title: "Employees With No Performance Review",
    description: `You have <code>all_employees</code> and <code>perf_reviews</code>. Find employees who have <strong>no performance review in 2024</strong>.\n\nReturn: <code>emp_id</code>, <code>emp_name</code>, <code>department</code>`,
    schema: `CREATE TABLE all_employees (emp_id INT, emp_name TEXT, department TEXT);
CREATE TABLE perf_reviews (review_id INT, emp_id INT, review_date TEXT, rating TEXT);
INSERT INTO all_employees VALUES (1,'Alice','Eng'),(2,'Bob','Eng'),(3,'Carol','Sales'),(4,'David','Sales'),(5,'Eve','HR');
INSERT INTO perf_reviews VALUES (1,1,'2024-03-01','Good'),(2,3,'2024-04-15','Excellent'),(3,1,'2023-03-01','Good');`,
    hint: "NOT EXISTS with review_date LIKE '2024%'",
    solution: `SELECT emp_id, emp_name, department FROM all_employees e
WHERE NOT EXISTS (
  SELECT 1 FROM perf_reviews r 
  WHERE r.emp_id = e.emp_id AND r.review_date LIKE '2024%'
);`
  },
  {
    id: 80, pattern: 16, difficulty: "Hard",
    title: "Customers Active in Jan But Not Feb",
    description: `You have a <code>monthly_activity</code> table. Find customers who were <strong>active in January 2024 but NOT in February 2024</strong>.\n\nReturn: <code>customer_id</code>`,
    schema: `CREATE TABLE monthly_activity (id INT, customer_id INT, activity_month TEXT);
INSERT INTO monthly_activity VALUES
(1,101,'2024-01'),(2,102,'2024-01'),(3,103,'2024-01'),(4,104,'2024-01'),
(5,101,'2024-02'),(6,103,'2024-02'),(7,105,'2024-02');`,
    hint: "Find Jan customers, then use NOT EXISTS to exclude those also in Feb",
    solution: `SELECT DISTINCT customer_id FROM monthly_activity jan
WHERE activity_month = '2024-01'
AND NOT EXISTS (
  SELECT 1 FROM monthly_activity feb 
  WHERE feb.customer_id = jan.customer_id AND feb.activity_month = '2024-02'
) ORDER BY customer_id;`
  },

  // ─── PATTERN 17: Cohort Analysis ──────────────────────────────────────────
  {
    id: 81, pattern: 17, difficulty: "Medium",
    title: "Assign Users to Monthly Cohorts",
    description: `You have a <code>cohort_signups</code> table. Assign each user to a cohort based on their <strong>signup month</strong>.\n\nReturn: <code>user_id</code>, <code>signup_date</code>, <code>cohort</code>`,
    schema: `CREATE TABLE cohort_signups (user_id INT, signup_date TEXT);
INSERT INTO cohort_signups VALUES
(101,'2024-01-05'),(102,'2024-01-20'),(103,'2024-02-03'),(104,'2024-02-15'),
(105,'2024-03-01'),(106,'2024-01-15');`,
    hint: "DATE_FORMAT(signup_date, '%Y-%m') gives the cohort month",
    solution: `SELECT user_id, signup_date,
  DATE_FORMAT(signup_date, '%Y-%m') AS cohort
FROM cohort_signups ORDER BY cohort, signup_date;`
  },
  {
    id: 82, pattern: 17, difficulty: "Medium",
    title: "Cohort Size and Revenue per Cohort",
    description: `You have <code>cohort_users</code> and <code>cohort_purchases</code>. For each cohort, show <strong>cohort size and total revenue</strong>.\n\nReturn: <code>cohort</code>, <code>cohort_size</code>, <code>total_revenue</code>, <code>revenue_per_user</code>`,
    schema: `CREATE TABLE cohort_users2 (user_id INT, signup_date TEXT);
CREATE TABLE cohort_purchases2 (id INT, user_id INT, amount INT);
INSERT INTO cohort_users2 VALUES (1,'2024-01-10'),(2,'2024-01-20'),(3,'2024-02-05'),(4,'2024-02-15'),(5,'2024-02-25');
INSERT INTO cohort_purchases2 VALUES (1,1,100),(2,1,200),(3,2,150),(4,3,300),(5,4,100);`,
    hint: "JOIN users to purchases, GROUP BY cohort month, SUM revenue, COUNT DISTINCT users",
    solution: `SELECT DATE_FORMAT(u.signup_date, '%Y-%m') AS cohort,
  COUNT(DISTINCT u.user_id) AS cohort_size,
  COALESCE(SUM(p.amount),0) AS total_revenue,
  ROUND(COALESCE(SUM(p.amount),0)*1.0/COUNT(DISTINCT u.user_id),2) AS revenue_per_user
FROM cohort_users2 u LEFT JOIN cohort_purchases2 p ON u.user_id=p.user_id
GROUP BY cohort ORDER BY cohort;`
  },
  {
    id: 83, pattern: 17, difficulty: "Hard",
    title: "Cohort Retention Table (Month 0, 1, 2)",
    description: `You have <code>cohort_members</code> (signup) and <code>cohort_logins</code> (activity). Build a <strong>cohort retention table</strong> showing % of each cohort still active at Month 0, 1, and 2.\n\nReturn: <code>cohort</code>, <code>cohort_size</code>, <code>month_0</code>, <code>month_1</code>, <code>month_2</code>`,
    schema: `CREATE TABLE cohort_members (user_id INT, signup_date TEXT);
CREATE TABLE cohort_logins (user_id INT, login_date TEXT);
INSERT INTO cohort_members VALUES (1,'2024-01-05'),(2,'2024-01-10'),(3,'2024-01-20'),(4,'2024-02-05'),(5,'2024-02-15');
INSERT INTO cohort_logins VALUES (1,'2024-01-05'),(1,'2024-02-10'),(1,'2024-03-05'),(2,'2024-01-10'),(2,'2024-02-20'),(3,'2024-01-20'),(4,'2024-02-05'),(4,'2024-03-10'),(5,'2024-02-15');`,
    hint: "Calculate months_since_signup = months between login_date and signup_date, then pivot",
    solution: `WITH base AS (
  SELECT m.user_id, DATE_FORMAT(m.signup_date, '%Y-%m') AS cohort, m.signup_date, l.login_date,
    (YEAR(l.login_date)-YEAR(m.signup_date))*12 + 
    (MONTH(l.login_date)-MONTH(m.signup_date)) AS months_since
  FROM cohort_members m LEFT JOIN cohort_logins l ON m.user_id=l.user_id
)
SELECT cohort,
  COUNT(DISTINCT user_id) AS cohort_size,
  COUNT(DISTINCT CASE WHEN months_since=0 THEN user_id END) AS month_0,
  COUNT(DISTINCT CASE WHEN months_since=1 THEN user_id END) AS month_1,
  COUNT(DISTINCT CASE WHEN months_since=2 THEN user_id END) AS month_2
FROM base GROUP BY cohort ORDER BY cohort;`
  },
  {
    id: 84, pattern: 17, difficulty: "Hard",
    title: "Revenue per Cohort Over Time",
    description: `You have <code>c_users</code> (signup) and <code>c_orders</code> (orders with dates). Show <strong>total revenue per cohort per month after signup</strong>.\n\nReturn: <code>cohort</code>, <code>months_since_signup</code>, <code>revenue</code>`,
    schema: `CREATE TABLE c_users (user_id INT, signup_date TEXT);
CREATE TABLE c_orders (id INT, user_id INT, order_date TEXT, amount INT);
INSERT INTO c_users VALUES (1,'2024-01-10'),(2,'2024-01-15'),(3,'2024-02-05'),(4,'2024-02-20');
INSERT INTO c_orders VALUES (1,1,'2024-01-15',100),(2,1,'2024-02-10',200),(3,1,'2024-03-05',150),(4,2,'2024-02-01',300),(5,3,'2024-02-10',250),(6,3,'2024-03-05',200),(7,4,'2024-03-01',100);`,
    hint: "JOIN users to orders, compute months_since = month diff, GROUP BY cohort and months_since",
    solution: `SELECT DATE_FORMAT(u.signup_date, '%Y-%m') AS cohort,
  (YEAR(o.order_date)-YEAR(u.signup_date))*12 + 
  (MONTH(o.order_date)-MONTH(u.signup_date)) AS months_since_signup,
  SUM(o.amount) AS revenue
FROM c_users u JOIN c_orders o ON u.user_id=o.user_id
GROUP BY cohort, months_since_signup ORDER BY cohort, months_since_signup;`
  },
  {
    id: 85, pattern: 17, difficulty: "Hard",
    title: "Cohort LTV Calculation",
    description: `Using <code>ltv_users</code> and <code>ltv_orders</code>, calculate the <strong>cumulative Lifetime Value (LTV) per cohort</strong> at Month 1, 2, and 3.\n\nReturn: <code>cohort</code>, <code>cohort_size</code>, <code>ltv_month_1</code>, <code>ltv_month_2</code>, <code>ltv_month_3</code>`,
    schema: `CREATE TABLE ltv_users (user_id INT, signup_date TEXT);
CREATE TABLE ltv_orders (id INT, user_id INT, order_date TEXT, amount INT);
INSERT INTO ltv_users VALUES (1,'2024-01-05'),(2,'2024-01-10'),(3,'2024-01-15');
INSERT INTO ltv_orders VALUES 
(1,1,'2024-01-10',100),(2,1,'2024-02-05',200),(3,1,'2024-03-10',150),
(4,2,'2024-02-01',300),(5,2,'2024-03-05',100),
(6,3,'2024-01-20',50),(7,3,'2024-02-15',200),(8,3,'2024-03-20',300);`,
    hint: "Compute months_since for each order, then use SUM(CASE WHEN months_since <= N) to get cumulative LTV",
    solution: `WITH base AS (
  SELECT u.user_id, DATE_FORMAT(u.signup_date, '%Y-%m') AS cohort,
    (YEAR(o.order_date)-YEAR(u.signup_date))*12+(MONTH(o.order_date)-MONTH(u.signup_date)) AS months_since,
    o.amount
  FROM ltv_users u JOIN ltv_orders o ON u.user_id=o.user_id
)
SELECT cohort,
  (SELECT COUNT(*) FROM ltv_users WHERE DATE_FORMAT(signup_date, '%Y-%m')=cohort) AS cohort_size,
  ROUND(SUM(CASE WHEN months_since<=1 THEN amount ELSE 0 END)*1.0/COUNT(DISTINCT user_id),2) AS ltv_month_1,
  ROUND(SUM(CASE WHEN months_since<=2 THEN amount ELSE 0 END)*1.0/COUNT(DISTINCT user_id),2) AS ltv_month_2,
  ROUND(SUM(CASE WHEN months_since<=3 THEN amount ELSE 0 END)*1.0/COUNT(DISTINCT user_id),2) AS ltv_month_3
FROM base GROUP BY cohort ORDER BY cohort;`
  },

  // ─── PATTERN 18: Window Filtering (Subquery Pattern) ──────────────────────
  {
    id: 86, pattern: 18, difficulty: "Easy",
    title: "Latest Record per Group (Subquery)",
    description: `You have a <code>records</code> table. Using the subquery/window filtering pattern, return the <strong>latest record per category</strong>.\n\nReturn: <code>category</code>, <code>value</code>, <code>record_date</code>`,
    schema: `CREATE TABLE records (id INT, category TEXT, value INT, record_date TEXT);
INSERT INTO records VALUES
(1,'A',100,'2024-01-01'),(2,'A',150,'2024-02-01'),(3,'A',120,'2024-03-01'),
(4,'B',200,'2024-01-15'),(5,'B',250,'2024-02-15'),
(6,'C',80,'2024-01-10'),(7,'C',90,'2024-03-10');`,
    hint: "SELECT * FROM (SELECT *, ROW_NUMBER() OVER (...) AS rn FROM records) t WHERE rn = 1",
    solution: `SELECT category, value, record_date FROM (
  SELECT *, ROW_NUMBER() OVER (PARTITION BY category ORDER BY record_date DESC) AS rn
  FROM records
) t WHERE rn = 1 ORDER BY category;`
  },
  {
    id: 87, pattern: 18, difficulty: "Medium",
    title: "2nd Highest Salary per Department",
    description: `You have an <code>dept_employees</code> table. Find the <strong>employee with the 2nd highest salary</strong> in each department using the window filtering pattern.\n\nReturn: <code>department</code>, <code>name</code>, <code>salary</code>`,
    schema: `CREATE TABLE dept_employees (id INT, name TEXT, department TEXT, salary INT);
INSERT INTO dept_employees VALUES
(1,'Alice','Eng',95000),(2,'Bob','Eng',88000),(3,'Carol','Eng',92000),(4,'Dan','Eng',88000),
(5,'Eve','Sales',75000),(6,'Frank','Sales',80000),(7,'Grace','Sales',70000);`,
    hint: "Use DENSE_RANK so tied 1st-place salaries still let a true 2nd appear, filter WHERE rn = 2",
    solution: `SELECT department, name, salary FROM (
  SELECT *, DENSE_RANK() OVER (PARTITION BY department ORDER BY salary DESC) AS rn
  FROM dept_employees
) t WHERE rn = 2 ORDER BY department;`
  },
  {
    id: 88, pattern: 18, difficulty: "Medium",
    title: "Filter on Window Aggregate (Top Half Revenue)",
    description: `You have a <code>rep_sales</code> table. Return only reps whose revenue is <strong>above the average revenue for their region</strong>.\n\nReturn: <code>region</code>, <code>rep_name</code>, <code>revenue</code>, <code>region_avg</code>`,
    schema: `CREATE TABLE rep_sales (id INT, rep_name TEXT, region TEXT, revenue INT);
INSERT INTO rep_sales VALUES
(1,'Alice','North',12000),(2,'Bob','North',8000),(3,'Carol','North',15000),
(4,'Dave','South',9000),(5,'Eve','South',11000),(6,'Frank','South',7000);`,
    hint: "Compute AVG(revenue) OVER (PARTITION BY region) in a subquery, then filter in outer query",
    solution: `SELECT region, rep_name, revenue, region_avg FROM (
  SELECT *, ROUND(AVG(revenue) OVER (PARTITION BY region),2) AS region_avg
  FROM rep_sales
) t WHERE revenue > region_avg ORDER BY region, revenue DESC;`
  },
  {
    id: 89, pattern: 18, difficulty: "Hard",
    title: "Every 3rd Transaction per User",
    description: `You have a <code>user_txns</code> table. Return every <strong>3rd transaction</strong> (by date) per user.\n\nReturn: <code>user_id</code>, <code>txn_id</code>, <code>txn_date</code>, <code>amount</code>`,
    schema: `CREATE TABLE user_txns (txn_id INT, user_id INT, txn_date TEXT, amount INT);
INSERT INTO user_txns VALUES
(1,101,'2024-01-01',100),(2,101,'2024-01-05',200),(3,101,'2024-01-10',150),(4,101,'2024-01-15',300),(5,101,'2024-01-20',250),(6,101,'2024-01-25',180),
(7,102,'2024-01-02',120),(8,102,'2024-01-08',90),(9,102,'2024-01-14',200);`,
    hint: "ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY txn_date), then WHERE rn % 3 = 0",
    solution: `SELECT user_id, txn_id, txn_date, amount FROM (
  SELECT *, ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY txn_date) AS rn
  FROM user_txns
) t WHERE rn % 3 = 0 ORDER BY user_id, txn_date;`
  },
  {
    id: 90, pattern: 18, difficulty: "Hard",
    title: "Top Product Per Category With Minimum Sales Threshold",
    description: `You have a <code>cat_products</code> table. Find the top-revenue product per category, but <strong>only include categories with at least 3 products and total revenue > 10000</strong>.\n\nReturn: <code>category</code>, <code>product_name</code>, <code>revenue</code>`,
    schema: `CREATE TABLE cat_products (id INT, category TEXT, product_name TEXT, revenue INT);
INSERT INTO cat_products VALUES
(1,'Electronics','Laptop',8000),(2,'Electronics','Phone',5000),(3,'Electronics','Tablet',3000),(4,'Electronics','Watch',1500),
(5,'Clothing','Jacket',2000),(6,'Clothing','Shirt',500),
(7,'Books','Novel',300),(8,'Books','Textbook',800),(9,'Books','Guide',400);`,
    hint: "First filter categories by product count >= 3 AND SUM(revenue) > 10000, then get top product per qualifying category",
    solution: `WITH qualified_cats AS (
  SELECT category FROM cat_products 
  GROUP BY category HAVING COUNT(*) >= 3 AND SUM(revenue) > 10000
),
ranked AS (
  SELECT *, ROW_NUMBER() OVER (PARTITION BY category ORDER BY revenue DESC) AS rn
  FROM cat_products WHERE category IN (SELECT category FROM qualified_cats)
)
SELECT category, product_name, revenue FROM ranked WHERE rn = 1 ORDER BY category;`
  },

  // ─── PATTERN 19: All Conditions Must Hold ─────────────────────────────────
  {
    id: 91, pattern: 19, difficulty: "Medium",
    title: "Users Active Every Month in Q1",
    description: `You have a <code>q1_activity</code> table. Find users who were <strong>active in ALL 3 months of Q1</strong> (Jan, Feb, March).\n\nReturn: <code>user_id</code>`,
    schema: `CREATE TABLE q1_activity (id INT, user_id INT, active_month TEXT);
INSERT INTO q1_activity VALUES
(1,101,'2024-01'),(2,101,'2024-02'),(3,101,'2024-03'),
(4,102,'2024-01'),(5,102,'2024-02'),
(6,103,'2024-01'),(7,103,'2024-02'),(8,103,'2024-03'),
(9,104,'2024-03');`,
    hint: "GROUP BY user_id, HAVING COUNT(DISTINCT active_month) = 3",
    solution: `SELECT user_id FROM q1_activity
WHERE active_month IN ('2024-01','2024-02','2024-03')
GROUP BY user_id
HAVING COUNT(DISTINCT active_month) = 3
ORDER BY user_id;`
  },
  {
    id: 92, pattern: 19, difficulty: "Medium",
    title: "Customers With No Failed Payments",
    description: `You have a <code>payment_log</code> table. Find customers who have <strong>only successful payments</strong> (no failures).\n\nReturn: <code>customer_id</code>`,
    schema: `CREATE TABLE payment_log (id INT, customer_id INT, status TEXT, amount INT);
INSERT INTO payment_log VALUES
(1,201,'success',100),(2,201,'success',200),(3,202,'success',150),(4,202,'failed',50),
(5,203,'success',300),(6,204,'failed',75),(7,204,'success',100),(8,205,'success',80);`,
    hint: "HAVING SUM(CASE WHEN status='failed' THEN 1 ELSE 0 END) = 0",
    solution: `SELECT customer_id FROM payment_log
GROUP BY customer_id
HAVING SUM(CASE WHEN status='failed' THEN 1 ELSE 0 END) = 0
ORDER BY customer_id;`
  },
  {
    id: 93, pattern: 19, difficulty: "Medium",
    title: "Products Always In Stock (Never Zero)",
    description: `You have a <code>inventory_checks</code> table with daily stock levels. Find products that have <strong>never had zero stock</strong>.\n\nReturn: <code>product_id</code>`,
    schema: `CREATE TABLE inventory_checks (id INT, product_id TEXT, check_date TEXT, stock_level INT);
INSERT INTO inventory_checks VALUES
(1,'P1','2024-01-01',50),(2,'P1','2024-01-02',30),(3,'P1','2024-01-03',20),
(4,'P2','2024-01-01',100),(5,'P2','2024-01-02',0),(6,'P2','2024-01-03',50),
(7,'P3','2024-01-01',75),(8,'P3','2024-01-02',60),(9,'P3','2024-01-03',45);`,
    hint: "HAVING MIN(stock_level) > 0 or HAVING SUM(CASE WHEN stock_level = 0 THEN 1 ELSE 0 END) = 0",
    solution: `SELECT product_id FROM inventory_checks
GROUP BY product_id
HAVING MIN(stock_level) > 0
ORDER BY product_id;`
  },
  {
    id: 94, pattern: 19, difficulty: "Hard",
    title: "Employees Who Passed All Required Trainings",
    description: `You have <code>required_trainings</code> and <code>completed_trainings</code>. Find employees who have <strong>completed ALL required trainings</strong>.\n\nReturn: <code>emp_id</code>`,
    schema: `CREATE TABLE required_trainings (training_id TEXT);
CREATE TABLE completed_trainings (emp_id INT, training_id TEXT);
INSERT INTO required_trainings VALUES ('Safety'),('Ethics'),('Security');
INSERT INTO completed_trainings VALUES 
(1,'Safety'),(1,'Ethics'),(1,'Security'),
(2,'Safety'),(2,'Ethics'),
(3,'Safety'),(3,'Ethics'),(3,'Security'),
(4,'Security');`,
    hint: "COUNT DISTINCT completed trainings = (SELECT COUNT(*) FROM required_trainings), filtered to only required ones",
    solution: `SELECT emp_id FROM completed_trainings
WHERE training_id IN (SELECT training_id FROM required_trainings)
GROUP BY emp_id
HAVING COUNT(DISTINCT training_id) = (SELECT COUNT(*) FROM required_trainings)
ORDER BY emp_id;`
  },
  {
    id: 95, pattern: 19, difficulty: "Hard",
    title: "Accounts Always Positive Balance",
    description: `You have an <code>account_balances</code> table with daily snapshots. Find accounts that have <strong>never had a negative balance</strong>.\n\nReturn: <code>account_id</code>, <code>min_balance</code>`,
    schema: `CREATE TABLE account_balances (id INT, account_id TEXT, balance_date TEXT, balance DECIMAL);
INSERT INTO account_balances VALUES
(1,'ACC1','2024-01-01',1000),(2,'ACC1','2024-01-02',850),(3,'ACC1','2024-01-03',200),
(4,'ACC2','2024-01-01',500),(5,'ACC2','2024-01-02',-50),(6,'ACC2','2024-01-03',100),
(7,'ACC3','2024-01-01',2000),(8,'ACC3','2024-01-02',1500),(9,'ACC3','2024-01-03',300);`,
    hint: "GROUP BY account_id, HAVING MIN(balance) >= 0",
    solution: `SELECT account_id, MIN(balance) AS min_balance
FROM account_balances
GROUP BY account_id
HAVING MIN(balance) >= 0
ORDER BY account_id;`
  },

  // ─── PATTERN 20: Rolling Retention / Activity ─────────────────────────────
  {
    id: 96, pattern: 20, difficulty: "Medium",
    title: "7-Day Rolling Active Users",
    description: `You have a <code>daily_active</code> table. For each day, count the number of <strong>distinct users active in the past 7 days</strong> (rolling window).\n\nReturn: <code>activity_date</code>, <code>rolling_7d_users</code>`,
    schema: `CREATE TABLE daily_active (id INT, user_id INT, activity_date TEXT);
INSERT INTO daily_active VALUES
(1,101,'2024-01-01'),(2,102,'2024-01-01'),(3,101,'2024-01-02'),(4,103,'2024-01-03'),
(5,104,'2024-01-04'),(6,101,'2024-01-05'),(7,102,'2024-01-06'),(8,105,'2024-01-07'),
(9,101,'2024-01-07'),(10,103,'2024-01-08');`,
    hint: "Get distinct user-date combos, then COUNT DISTINCT users WHERE date is within 7-day window",
    solution: `WITH daily AS (SELECT DISTINCT activity_date, user_id FROM daily_active),
dates AS (SELECT DISTINCT activity_date FROM daily_active)
SELECT d.activity_date,
  COUNT(DISTINCT a.user_id) AS rolling_7d_users
FROM dates d
JOIN daily a ON a.activity_date BETWEEN DATE_SUB(d.activity_date, INTERVAL 6 DAY) AND d.activity_date
GROUP BY d.activity_date ORDER BY d.activity_date;`
  },
  {
    id: 97, pattern: 20, difficulty: "Medium",
    title: "30-Day Rolling Revenue",
    description: `You have a <code>daily_rev</code> table. For each day, calculate the <strong>sum of revenue over the past 30 days</strong>.\n\nReturn: <code>rev_date</code>, <code>daily_revenue</code>, <code>rolling_30d_revenue</code>`,
    schema: `CREATE TABLE daily_rev (id INT, rev_date TEXT, revenue INT);
INSERT INTO daily_rev VALUES
(1,'2024-01-01',1000),(2,'2024-01-05',1500),(3,'2024-01-10',800),
(4,'2024-01-15',2000),(5,'2024-01-20',1200),(6,'2024-02-01',1800),(7,'2024-02-10',900);`,
    hint: "Self join where b.rev_date BETWEEN DATE_SUB(a.rev_date, INTERVAL 29 DAY) AND a.rev_date",
    solution: `SELECT a.rev_date, a.revenue AS daily_revenue,
  SUM(b.revenue) AS rolling_30d_revenue
FROM daily_rev a
JOIN daily_rev b ON b.rev_date BETWEEN DATE_SUB(a.rev_date, INTERVAL 29 DAY) AND a.rev_date
GROUP BY a.rev_date ORDER BY a.rev_date;`
  },
  {
    id: 98, pattern: 20, difficulty: "Hard",
    title: "Weekly Active Users (WAU) Over Time",
    description: `You have a <code>wau_events</code> table. For each week, count users active in <strong>that calendar week</strong> (Mon–Sun).\n\nReturn: <code>week_start</code>, <code>wau</code>`,
    schema: `CREATE TABLE wau_events (id INT, user_id INT, event_date TEXT);
INSERT INTO wau_events VALUES
(1,101,'2024-01-01'),(2,102,'2024-01-02'),(3,101,'2024-01-03'),(4,103,'2024-01-04'),
(5,101,'2024-01-08'),(6,104,'2024-01-09'),(7,102,'2024-01-10'),
(8,101,'2024-01-15'),(9,103,'2024-01-15'),(10,105,'2024-01-16');`,
    hint: "Use strftime to get week number, GROUP BY week and COUNT DISTINCT users",
    solution: `SELECT DATE_FORMAT(event_date, '%Y-W%u') AS week_start,
  COUNT(DISTINCT user_id) AS wau
FROM wau_events
GROUP BY DATE_FORMAT(event_date, '%Y-W%u')
ORDER BY week_start;`
  },
  {
    id: 99, pattern: 20, difficulty: "Hard",
    title: "Monthly Active Users (MAU) and MoM Change",
    description: `You have a <code>mau_events</code> table. Calculate <strong>MAU per month and the month-over-month change</strong>.\n\nReturn: <code>month</code>, <code>mau</code>, <code>prev_mau</code>, <code>mau_change</code>`,
    schema: `CREATE TABLE mau_events (id INT, user_id INT, event_date TEXT);
INSERT INTO mau_events VALUES
(1,101,'2024-01-05'),(2,102,'2024-01-10'),(3,103,'2024-01-15'),(4,101,'2024-01-20'),
(5,101,'2024-02-01'),(6,102,'2024-02-05'),(7,104,'2024-02-10'),(8,103,'2024-02-15'),(9,105,'2024-02-20'),
(10,101,'2024-03-05'),(11,105,'2024-03-10'),(12,106,'2024-03-15');`,
    hint: "First get MAU per month with COUNT DISTINCT, then use LAG to get previous month",
    solution: `WITH monthly AS (
  SELECT DATE_FORMAT(event_date, '%Y-%m') AS month, COUNT(DISTINCT user_id) AS mau
  FROM mau_events GROUP BY month
)
SELECT month, mau,
  LAG(mau) OVER (ORDER BY month) AS prev_mau,
  mau - LAG(mau) OVER (ORDER BY month) AS mau_change
FROM monthly ORDER BY month;`
  },
  {
    id: 100, pattern: 20, difficulty: "Hard",
    title: "Stickiness Ratio (DAU/MAU)",
    description: `You have a <code>stickiness_events</code> table. Calculate the <strong>DAU/MAU stickiness ratio</strong> for each day (DAU on that day divided by MAU for that month).\n\nReturn: <code>event_date</code>, <code>dau</code>, <code>mau</code>, <code>stickiness</code>`,
    schema: `CREATE TABLE stickiness_events (id INT, user_id INT, event_date TEXT);
INSERT INTO stickiness_events VALUES
(1,101,'2024-01-01'),(2,102,'2024-01-01'),(3,103,'2024-01-02'),(4,101,'2024-01-02'),
(5,104,'2024-01-05'),(6,101,'2024-01-05'),(7,102,'2024-01-10'),(8,105,'2024-01-10'),
(9,101,'2024-02-01'),(10,102,'2024-02-01'),(11,106,'2024-02-15');`,
    hint: "Join DAU (per day) with MAU (per month), then divide. MAU = COUNT DISTINCT users in same month",
    solution: `WITH dau AS (
  SELECT event_date, COUNT(DISTINCT user_id) AS daily_users FROM stickiness_events GROUP BY event_date
),
mau AS (
  SELECT DATE_FORMAT(event_date, '%Y-%m') AS month, COUNT(DISTINCT user_id) AS monthly_users FROM stickiness_events GROUP BY month
)
SELECT d.event_date, d.daily_users AS dau, m.monthly_users AS mau,
  ROUND(d.daily_users*1.0/m.monthly_users,4) AS stickiness
FROM dau d JOIN mau m ON DATE_FORMAT(d.event_date, '%Y-%m')=m.month
ORDER BY d.event_date;`
  }
];

// ─── MOCK INTERVIEW QUESTIONS (100) ───────────────────────────────────────────

const MOCK_QUESTIONS = [
  {
    id: 101, difficulty: "Easy", company: "Meta", topic: "Basic Aggregation",
    title: "Total Messages Sent per User",
    description: `You have a <code>messages</code> table. Find the <strong>total messages sent per user</strong>, ordered by count descending.\n\nReturn: <code>sender_id</code>, <code>message_count</code>`,
    schema: `CREATE TABLE messages (msg_id INT, sender_id INT, receiver_id INT, sent_at TEXT);
INSERT INTO messages VALUES (1,101,102,'2024-01-01'),(2,101,103,'2024-01-02'),(3,102,101,'2024-01-02'),(4,103,101,'2024-01-03'),(5,101,104,'2024-01-03'),(6,102,103,'2024-01-04');`,
    hint: "Simple GROUP BY with COUNT",
    solution: `SELECT sender_id, COUNT(*) AS message_count FROM messages GROUP BY sender_id ORDER BY message_count DESC;`
  },
  {
    id: 102, difficulty: "Easy", company: "Amazon", topic: "Filtering",
    title: "Orders Above Average Order Value",
    description: `You have an <code>ecom_orders</code> table. Return all orders where the amount is <strong>above the overall average order value</strong>.\n\nReturn: <code>order_id</code>, <code>customer_id</code>, <code>amount</code>`,
    schema: `CREATE TABLE ecom_orders (order_id INT, customer_id INT, amount DECIMAL);
INSERT INTO ecom_orders VALUES (1,101,150),(2,102,300),(3,103,75),(4,104,500),(5,105,200),(6,106,100),(7,107,450);`,
    hint: "WHERE amount > (SELECT AVG(amount) FROM ecom_orders)",
    solution: `SELECT order_id, customer_id, amount FROM ecom_orders WHERE amount > (SELECT AVG(amount) FROM ecom_orders) ORDER BY amount DESC;`
  },
  {
    id: 103, difficulty: "Easy", company: "Google", topic: "Date Functions",
    title: "Signups in the Last 30 Days",
    description: `You have a <code>new_users</code> table. Count signups in <strong>each of the last 30 days</strong>. Use '2024-02-29' as today.\n\nReturn: <code>signup_date</code>, <code>signups</code>`,
    schema: `CREATE TABLE new_users (user_id INT, signup_date TEXT);
INSERT INTO new_users VALUES (1,'2024-02-01'),(2,'2024-02-05'),(3,'2024-02-10'),(4,'2024-02-10'),(5,'2024-02-15'),(6,'2024-02-20'),(7,'2024-01-25'),(8,'2024-02-25');`,
    hint: "WHERE signup_date >= DATE_SUB('2024-02-29', INTERVAL 30 DAY) GROUP BY signup_date",
    solution: `SELECT signup_date, COUNT(*) AS signups FROM new_users WHERE signup_date >= DATE_SUB('2024-02-29', INTERVAL 30 DAY) GROUP BY signup_date ORDER BY signup_date;`
  },
  {
    id: 104, difficulty: "Easy", company: "Uber", topic: "JOINs",
    title: "Completed Trips with Driver Info",
    description: `You have <code>trips</code> and <code>drivers</code> tables. Return all <strong>completed trips</strong> with the driver's name.\n\nReturn: <code>trip_id</code>, <code>driver_name</code>, <code>fare</code>`,
    schema: `CREATE TABLE trips (trip_id INT, driver_id INT, status TEXT, fare DECIMAL);
CREATE TABLE drivers (driver_id INT, driver_name TEXT);
INSERT INTO trips VALUES (1,10,'completed',15.50),(2,11,'cancelled',0),(3,10,'completed',22.00),(4,12,'completed',8.75),(5,11,'completed',30.00);
INSERT INTO drivers VALUES (10,'Alice'),(11,'Bob'),(12,'Carol');`,
    hint: "JOIN trips to drivers on driver_id, WHERE status = 'completed'",
    solution: `SELECT t.trip_id, d.driver_name, t.fare FROM trips t JOIN drivers d ON t.driver_id=d.driver_id WHERE t.status='completed' ORDER BY t.trip_id;`
  },
  {
    id: 105, difficulty: "Easy", company: "LinkedIn", topic: "Aggregation",
    title: "Average Connections per User",
    description: `You have a <code>connections</code> table. Calculate the <strong>average number of connections per user</strong>.\n\nReturn: <code>avg_connections</code>`,
    schema: `CREATE TABLE connections (user_id INT, connection_count INT);
INSERT INTO connections VALUES (1,50),(2,120),(3,30),(4,200),(5,75),(6,90);`,
    hint: "SELECT AVG(connection_count)",
    solution: `SELECT ROUND(AVG(connection_count),2) AS avg_connections FROM connections;`
  },
  {
    id: 106, difficulty: "Medium", company: "Netflix", topic: "Window Functions",
    title: "Top 3 Most Watched Shows per Genre",
    description: `You have a <code>watch_stats</code> table. Find the <strong>top 3 most watched shows per genre</strong>.\n\nReturn: <code>genre</code>, <code>show_name</code>, <code>watch_hours</code>`,
    schema: `CREATE TABLE watch_stats (id INT, show_name TEXT, genre TEXT, watch_hours INT);
INSERT INTO watch_stats VALUES (1,'Show A','Drama',5000),(2,'Show B','Drama',7000),(3,'Show C','Drama',6000),(4,'Show D','Drama',4000),(5,'Show E','Comedy',8000),(6,'Show F','Comedy',3000),(7,'Show G','Comedy',6500),(8,'Show H','Comedy',5500),(9,'Show I','Thriller',9000),(10,'Show J','Thriller',4000);`,
    hint: "ROW_NUMBER() OVER (PARTITION BY genre ORDER BY watch_hours DESC), filter rn <= 3",
    solution: `SELECT genre, show_name, watch_hours FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY genre ORDER BY watch_hours DESC) AS rn FROM watch_stats) t WHERE rn <= 3 ORDER BY genre, watch_hours DESC;`
  },
  {
    id: 107, difficulty: "Medium", company: "Airbnb", topic: "Aggregation + Filtering",
    title: "Hosts with Average Rating Above 4.5",
    description: `You have a <code>reviews</code> table. Find hosts with <strong>at least 5 reviews and an average rating above 4.5</strong>.\n\nReturn: <code>host_id</code>, <code>review_count</code>, <code>avg_rating</code>`,
    schema: `CREATE TABLE reviews (review_id INT, host_id INT, rating DECIMAL);
INSERT INTO reviews VALUES (1,201,4.8),(2,201,4.9),(3,201,4.7),(4,201,5.0),(5,201,4.6),(6,202,4.2),(7,202,4.5),(8,202,4.0),(9,203,4.9),(10,203,5.0),(11,203,4.8),(12,203,4.7),(13,203,4.9),(14,204,3.5),(15,204,4.0);`,
    hint: "GROUP BY host_id, HAVING COUNT(*) >= 5 AND AVG(rating) > 4.5",
    solution: `SELECT host_id, COUNT(*) AS review_count, ROUND(AVG(rating),2) AS avg_rating FROM reviews GROUP BY host_id HAVING COUNT(*) >= 5 AND AVG(rating) > 4.5 ORDER BY avg_rating DESC;`
  },
  {
    id: 108, difficulty: "Medium", company: "Twitter/X", topic: "Self Join",
    title: "Users Followed by Both User A and User B",
    description: `You have a <code>tw_follows</code> table. Find users followed by <strong>both user 101 and user 102</strong>.\n\nReturn: <code>followed_user_id</code>`,
    schema: `CREATE TABLE tw_follows (follower_id INT, followed_id INT);
INSERT INTO tw_follows VALUES (101,201),(101,202),(101,203),(101,204),(102,202),(102,203),(102,205),(103,201),(103,202);`,
    hint: "INTERSECT or self join filtering for each follower",
    solution: `SELECT followed_id AS followed_user_id FROM tw_follows WHERE follower_id=101
INTERSECT
SELECT followed_id FROM tw_follows WHERE follower_id=102;`
  },
  {
    id: 109, difficulty: "Medium", company: "Spotify", topic: "Ranking",
    title: "Artist Ranking by Streams This Month",
    description: `You have a <code>streams</code> table. Rank artists by <strong>total streams in January 2024</strong> using DENSE_RANK.\n\nReturn: <code>artist_name</code>, <code>total_streams</code>, <code>rank</code>`,
    schema: `CREATE TABLE streams (id INT, artist_name TEXT, stream_date TEXT, streams INT);
INSERT INTO streams VALUES (1,'Artist A','2024-01-05',50000),(2,'Artist B','2024-01-10',80000),(3,'Artist A','2024-01-15',60000),(4,'Artist C','2024-01-08',90000),(5,'Artist B','2024-01-20',70000),(6,'Artist D','2024-01-12',80000),(7,'Artist C','2024-01-25',50000);`,
    hint: "SUM streams WHERE month = Jan 2024, then DENSE_RANK on total",
    solution: `SELECT artist_name, total_streams, DENSE_RANK() OVER (ORDER BY total_streams DESC) AS rank FROM (SELECT artist_name, SUM(streams) AS total_streams FROM streams WHERE stream_date LIKE '2024-01%' GROUP BY artist_name) ORDER BY rank;`
  },
  {
    id: 110, difficulty: "Medium", company: "DoorDash", topic: "Multiple JOINs",
    title: "Average Delivery Time by Restaurant",
    description: `You have <code>deliveries</code> and <code>restaurants</code>. Calculate <strong>average delivery time in minutes</strong> per restaurant.\n\nReturn: <code>restaurant_name</code>, <code>avg_delivery_mins</code>`,
    schema: `CREATE TABLE deliveries (del_id INT, restaurant_id INT, placed_at TEXT, delivered_at TEXT);
CREATE TABLE restaurants (restaurant_id INT, restaurant_name TEXT);
INSERT INTO deliveries VALUES (1,1,'2024-01-01 12:00','2024-01-01 12:35'),(2,1,'2024-01-01 13:00','2024-01-01 13:40'),(3,2,'2024-01-01 12:00','2024-01-01 12:25'),(4,2,'2024-01-01 14:00','2024-01-01 14:30'),(5,3,'2024-01-01 12:00','2024-01-01 12:45');
INSERT INTO restaurants VALUES (1,'Burger Place'),(2,'Pizza Palace'),(3,'Sushi Spot');`,
    hint: "TIMESTAMPDIFF(MINUTE, placed_at, delivered_at) gives minutes",
    solution: `SELECT r.restaurant_name, ROUND(AVG(TIMESTAMPDIFF(MINUTE, d.placed_at, d.delivered_at)),2) AS avg_delivery_mins FROM deliveries d JOIN restaurants r ON d.restaurant_id=r.restaurant_id GROUP BY r.restaurant_name ORDER BY avg_delivery_mins;`
  },
  {
    id: 111, difficulty: "Medium", company: "Stripe", topic: "Running Total",
    title: "Running Payment Volume per Merchant",
    description: `You have a <code>payments</code> table. Show a <strong>running total of payment volume per merchant</strong> ordered by date.\n\nReturn: <code>merchant_id</code>, <code>payment_date</code>, <code>amount</code>, <code>running_volume</code>`,
    schema: `CREATE TABLE merchant_payments (id INT, merchant_id INT, amount DECIMAL, payment_date TEXT);
INSERT INTO merchant_payments VALUES (1,'M1',500,'2024-01-01'),(2,'M1',750,'2024-01-05'),(3,'M1',300,'2024-01-10'),(4,'M2',1000,'2024-01-02'),(5,'M2',250,'2024-01-08'),(6,'M2',500,'2024-01-12');`,
    hint: "SUM(amount) OVER (PARTITION BY merchant_id ORDER BY payment_date)",
    solution: `SELECT merchant_id, payment_date, amount, SUM(amount) OVER (PARTITION BY merchant_id ORDER BY payment_date) AS running_volume FROM merchant_payments ORDER BY merchant_id, payment_date;`
  },
  {
    id: 112, difficulty: "Medium", company: "Lyft", topic: "Cohort",
    title: "Driver Cohort — First Trip Month",
    description: `You have a <code>driver_trips</code> table. Identify which <strong>cohort (first active month) each driver belongs to</strong>.\n\nReturn: <code>driver_id</code>, <code>cohort_month</code>`,
    schema: `CREATE TABLE driver_trips (id INT, driver_id INT, trip_date TEXT);
INSERT INTO driver_trips VALUES (1,10,'2024-01-05'),(2,10,'2024-02-10'),(3,11,'2024-02-01'),(4,11,'2024-02-20'),(5,12,'2024-01-15'),(6,12,'2024-03-05'),(7,13,'2024-03-01');`,
    hint: "MIN(trip_date) per driver, then extract month",
    solution: `SELECT driver_id, DATE_FORMAT(MIN(trip_date, '%Y-%m')) AS cohort_month FROM driver_trips GROUP BY driver_id ORDER BY driver_id;`
  },
  {
    id: 113, difficulty: "Medium", company: "Slack", topic: "Retention",
    title: "Weekly Message Retention",
    description: `You have a <code>slack_messages</code> table. Find users who sent messages in <strong>both week 1 (Jan 1-7) and week 2 (Jan 8-14)</strong>.\n\nReturn: <code>user_id</code>`,
    schema: `CREATE TABLE slack_messages (id INT, user_id INT, sent_date TEXT);
INSERT INTO slack_messages VALUES (1,101,'2024-01-02'),(2,102,'2024-01-05'),(3,103,'2024-01-07'),(4,101,'2024-01-09'),(5,102,'2024-01-12'),(6,104,'2024-01-10'),(7,103,'2024-01-14');`,
    hint: "INTERSECT or two EXISTS clauses for each week",
    solution: `SELECT user_id FROM slack_messages WHERE sent_date BETWEEN '2024-01-01' AND '2024-01-07'
INTERSECT
SELECT user_id FROM slack_messages WHERE sent_date BETWEEN '2024-01-08' AND '2024-01-14';`
  },
  {
    id: 114, difficulty: "Medium", company: "Pinterest", topic: "Top N",
    title: "Most Saved Pins per Board Category",
    description: `You have a <code>pins</code> table. Find the <strong>top 2 most saved pins per category</strong>.\n\nReturn: <code>category</code>, <code>pin_title</code>, <code>saves</code>`,
    schema: `CREATE TABLE pins (pin_id INT, pin_title TEXT, category TEXT, saves INT);
INSERT INTO pins VALUES (1,'Pin A','Food',500),(2,'Pin B','Food',800),(3,'Pin C','Food',300),(4,'Pin D','Travel',1200),(5,'Pin E','Travel',900),(6,'Pin F','Travel',600),(7,'Pin G','DIY',400),(8,'Pin H','DIY',700);`,
    hint: "ROW_NUMBER() OVER (PARTITION BY category ORDER BY saves DESC), filter rn <= 2",
    solution: `SELECT category, pin_title, saves FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY category ORDER BY saves DESC) AS rn FROM pins) t WHERE rn <= 2 ORDER BY category, saves DESC;`
  },
  {
    id: 115, difficulty: "Medium", company: "TikTok", topic: "Growth",
    title: "Week-over-Week Video Upload Growth",
    description: `You have a <code>video_uploads</code> table. Calculate the <strong>week-over-week change in uploads</strong>.\n\nReturn: <code>week</code>, <code>uploads</code>, <code>prev_week_uploads</code>, <code>wow_change_pct</code>`,
    schema: `CREATE TABLE video_uploads (id INT, upload_date TEXT);
INSERT INTO video_uploads VALUES (1,'2024-01-01'),(2,'2024-01-02'),(3,'2024-01-03'),(4,'2024-01-08'),(5,'2024-01-09'),(6,'2024-01-10'),(7,'2024-01-11'),(8,'2024-01-12'),(9,'2024-01-15');`,
    hint: "Group by week, LAG for previous week, compute % change",
    solution: `WITH weekly AS (SELECT DATE_FORMAT(upload_date, '%Y-W%u') AS week, COUNT(*) AS uploads FROM video_uploads GROUP BY week)
SELECT week, uploads, LAG(uploads) OVER (ORDER BY week) AS prev_week_uploads,
  ROUND((uploads-LAG(uploads) OVER (ORDER BY week))*100.0/LAG(uploads) OVER (ORDER BY week),2) AS wow_change_pct
FROM weekly ORDER BY week;`
  },
  {
    id: 116, difficulty: "Hard", company: "Meta", topic: "Friend Recommendations",
    title: "Friend-of-Friend Recommendations",
    description: `You have a <code>friendships</code> table (bidirectional). Find <strong>users who are friends-of-friends but not already friends</strong> with user 1.\n\nReturn: <code>recommended_user_id</code>, <code>mutual_friends</code>`,
    schema: `CREATE TABLE friendships (user_a INT, user_b INT);
INSERT INTO friendships VALUES (1,2),(1,3),(2,4),(2,5),(3,5),(3,6),(4,7);`,
    hint: "Find friends of user 1's friends, exclude user 1 and existing friends, count mutual connections",
    solution: `WITH user1_friends AS (SELECT user_b AS friend FROM friendships WHERE user_a=1 UNION SELECT user_a FROM friendships WHERE user_b=1),
fof AS (
  SELECT CASE WHEN f.user_a=uf.friend THEN f.user_b ELSE f.user_a END AS candidate
  FROM friendships f JOIN user1_friends uf ON f.user_a=uf.friend OR f.user_b=uf.friend
  WHERE CASE WHEN f.user_a=uf.friend THEN f.user_b ELSE f.user_a END != 1
)
SELECT candidate AS recommended_user_id, COUNT(*) AS mutual_friends FROM fof
WHERE candidate NOT IN (SELECT friend FROM user1_friends)
GROUP BY candidate ORDER BY mutual_friends DESC;`
  },
  {
    id: 117, difficulty: "Hard", company: "Amazon", topic: "Funnel + Attribution",
    title: "Cart Abandonment Rate by Category",
    description: `You have <code>cart_events</code> with steps: add_to_cart, purchase. Find the <strong>cart abandonment rate per product category</strong>.\n\nReturn: <code>category</code>, <code>added_to_cart</code>, <code>purchased</code>, <code>abandonment_rate</code>`,
    schema: `CREATE TABLE cart_events (id INT, user_id INT, category TEXT, step TEXT);
INSERT INTO cart_events VALUES (1,101,'Electronics','add_to_cart'),(2,101,'Electronics','purchase'),(3,102,'Electronics','add_to_cart'),(4,103,'Electronics','add_to_cart'),(5,104,'Clothing','add_to_cart'),(6,104,'Clothing','purchase'),(7,105,'Clothing','add_to_cart'),(8,106,'Clothing','add_to_cart'),(9,106,'Clothing','purchase');`,
    hint: "COUNT DISTINCT users at each step per category, then compute (added-purchased)/added",
    solution: `SELECT category,
  COUNT(DISTINCT CASE WHEN step='add_to_cart' THEN user_id END) AS added_to_cart,
  COUNT(DISTINCT CASE WHEN step='purchase' THEN user_id END) AS purchased,
  ROUND((COUNT(DISTINCT CASE WHEN step='add_to_cart' THEN user_id END)-COUNT(DISTINCT CASE WHEN step='purchase' THEN user_id END))*100.0/COUNT(DISTINCT CASE WHEN step='add_to_cart' THEN user_id END),2) AS abandonment_rate
FROM cart_events GROUP BY category ORDER BY abandonment_rate DESC;`
  },
  {
    id: 118, difficulty: "Hard", company: "Google", topic: "Search Analytics",
    title: "Most Common Search Query per Hour",
    description: `You have a <code>search_logs</code> table. Find the <strong>most common search query for each hour</strong> of the day.\n\nReturn: <code>hour</code>, <code>top_query</code>, <code>search_count</code>`,
    schema: `CREATE TABLE search_logs (id INT, query TEXT, searched_at TEXT);
INSERT INTO search_logs VALUES (1,'cats','2024-01-01 09:00'),(2,'dogs','2024-01-01 09:15'),(3,'cats','2024-01-01 09:30'),(4,'cats','2024-01-01 10:00'),(5,'weather','2024-01-01 10:15'),(6,'weather','2024-01-01 10:30'),(7,'weather','2024-01-01 10:45'),(8,'news','2024-01-01 11:00'),(9,'sports','2024-01-01 11:30'),(10,'news','2024-01-01 11:45');`,
    hint: "COUNT per hour+query, then ROW_NUMBER() OVER (PARTITION BY hour ORDER BY count DESC), filter rn=1",
    solution: `SELECT hour, top_query, search_count FROM (
  SELECT HOUR(searched_at) AS hour, query AS top_query, COUNT(*) AS search_count,
    ROW_NUMBER() OVER (PARTITION BY HOUR(searched_at) ORDER BY COUNT(*) DESC) AS rn
  FROM search_logs GROUP BY HOUR(searched_at), query
) WHERE rn=1 ORDER BY hour;`
  },
  {
    id: 119, difficulty: "Hard", company: "Uber", topic: "Driver Metrics",
    title: "Consecutive Days a Driver Completed Trips",
    description: `You have a <code>driver_completions</code> table. Find the <strong>longest streak of consecutive days</strong> each driver had at least one completion.\n\nReturn: <code>driver_id</code>, <code>max_streak</code>`,
    schema: `CREATE TABLE driver_completions (id INT, driver_id INT, completion_date TEXT);
INSERT INTO driver_completions VALUES (1,10,'2024-01-01'),(2,10,'2024-01-02'),(3,10,'2024-01-03'),(4,10,'2024-01-05'),(5,10,'2024-01-06'),(6,11,'2024-01-01'),(7,11,'2024-01-02'),(8,11,'2024-01-05'),(9,11,'2024-01-06'),(10,11,'2024-01-07'),(11,11,'2024-01-08');`,
    hint: "Deduplicate by driver+date, apply streak grouping (date - ROW_NUMBER), count per group, find MAX",
    solution: `WITH deduped AS (SELECT DISTINCT driver_id, completion_date FROM driver_completions),
groups AS (SELECT driver_id, completion_date, DATE(completion_date,CONCAT('−', (ROW_NUMBER() OVER (PARTITION BY driver_id ORDER BY completion_date)-1))||' days') AS grp FROM deduped)
SELECT driver_id, MAX(cnt) AS max_streak FROM (SELECT driver_id, grp, COUNT(*) AS cnt FROM groups GROUP BY driver_id, grp) GROUP BY driver_id ORDER BY driver_id;`
  },
  {
    id: 120, difficulty: "Hard", company: "Airbnb", topic: "Revenue Analysis",
    title: "Host Revenue in the Top 10%",
    description: `You have a <code>host_revenue</code> table. Find hosts whose total revenue places them in the <strong>top 10%</strong> of all hosts.\n\nReturn: <code>host_id</code>, <code>total_revenue</code>, <code>percentile</code>`,
    schema: `CREATE TABLE host_revenue (id INT, host_id INT, amount DECIMAL);
INSERT INTO host_revenue VALUES (1,201,5000),(2,201,3000),(3,202,8000),(4,203,1000),(5,204,12000),(6,205,2000),(7,206,15000),(8,207,500),(9,208,9000),(10,209,4000);`,
    hint: "SUM per host, then NTILE(10) OVER (ORDER BY total DESC), filter tile = 1",
    solution: `WITH totals AS (SELECT host_id, SUM(amount) AS total_revenue FROM host_revenue GROUP BY host_id)
SELECT host_id, total_revenue, NTILE(10) OVER (ORDER BY total_revenue DESC) AS percentile_bucket
FROM totals WHERE NTILE(10) OVER (ORDER BY total_revenue DESC) = 1 ORDER BY total_revenue DESC;`
  },
  {
    id: 121, difficulty: "Easy", company: "Facebook", topic: "Basic SQL",
    title: "Users With More Than 100 Friends",
    description: `You have a <code>fb_users</code> table. Return users with <strong>more than 100 friends</strong>.\n\nReturn: <code>user_id</code>, <code>friend_count</code>`,
    schema: `CREATE TABLE fb_users (user_id INT, name TEXT, friend_count INT);
INSERT INTO fb_users VALUES (1,'Alice',150),(2,'Bob',80),(3,'Carol',200),(4,'David',50),(5,'Eve',120);`,
    hint: "Simple WHERE filter",
    solution: `SELECT user_id, friend_count FROM fb_users WHERE friend_count > 100 ORDER BY friend_count DESC;`
  },
  {
    id: 122, difficulty: "Easy", company: "Shopify", topic: "NULL Handling",
    title: "Orders with Missing Shipping Address",
    description: `You have a <code>shop_orders</code> table. Find orders where <code>shipping_address</code> is <strong>NULL</strong>.\n\nReturn: <code>order_id</code>, <code>customer_id</code>, <code>amount</code>`,
    schema: `CREATE TABLE shop_orders (order_id INT, customer_id INT, amount DECIMAL, shipping_address TEXT);
INSERT INTO shop_orders VALUES (1,101,100,'123 Main St'),(2,102,200,NULL),(3,103,150,'456 Oak Ave'),(4,104,75,NULL),(5,105,300,'789 Pine Rd');`,
    hint: "WHERE shipping_address IS NULL",
    solution: `SELECT order_id, customer_id, amount FROM shop_orders WHERE shipping_address IS NULL;`
  },
  {
    id: 123, difficulty: "Easy", company: "Zoom", topic: "Date Math",
    title: "Meetings Longer Than 1 Hour",
    description: `You have a <code>meetings</code> table. Find all meetings that lasted <strong>more than 60 minutes</strong>.\n\nReturn: <code>meeting_id</code>, <code>host_id</code>, <code>duration_mins</code>`,
    schema: `CREATE TABLE meetings (meeting_id INT, host_id INT, start_time TEXT, end_time TEXT);
INSERT INTO meetings VALUES (1,101,'2024-01-01 09:00','2024-01-01 10:30'),(2,102,'2024-01-01 11:00','2024-01-01 11:45'),(3,103,'2024-01-01 14:00','2024-01-01 15:30'),(4,101,'2024-01-02 10:00','2024-01-02 11:05');`,
    hint: "TIMESTAMPDIFF(MINUTE, start_time, end_time) gives minutes",
    solution: `SELECT meeting_id, host_id, ROUND(TIMESTAMPDIFF(MINUTE, start_time, end_time),0) AS duration_mins FROM meetings WHERE TIMESTAMPDIFF(MINUTE, start_time, end_time) > 60;`
  },
  {
    id: 124, difficulty: "Easy", company: "Twilio", topic: "String Functions",
    title: "Messages Containing a Keyword",
    description: `You have an <code>sms_log</code> table. Find all messages containing the word <strong>'urgent'</strong> (case-insensitive).\n\nReturn: <code>msg_id</code>, <code>body</code>`,
    schema: `CREATE TABLE sms_log (msg_id INT, sender TEXT, body TEXT);
INSERT INTO sms_log VALUES (1,'Alice','Please call back URGENT'),(2,'Bob','Hello how are you'),(3,'Carol','This is urgent please respond'),(4,'David','Meeting at 3pm'),(5,'Eve','URGENT: server is down');`,
    hint: "WHERE LOWER(body) LIKE '%urgent%'",
    solution: `SELECT msg_id, body FROM sms_log WHERE LOWER(body) LIKE '%urgent%' ORDER BY msg_id;`
  },
  {
    id: 125, difficulty: "Easy", company: "HubSpot", topic: "Aggregation",
    title: "Leads by Source",
    description: `You have a <code>leads</code> table. Count the <strong>number of leads per source</strong>, ordered by count.\n\nReturn: <code>source</code>, <code>lead_count</code>`,
    schema: `CREATE TABLE leads (lead_id INT, source TEXT, created_date TEXT);
INSERT INTO leads VALUES (1,'organic'),(2,'paid'),(3,'organic'),(4,'referral'),(5,'paid'),(6,'paid'),(7,'organic'),(8,'referral'),(9,'direct'),(10,'paid');`,
    hint: "GROUP BY source, COUNT(*)",
    solution: `SELECT source, COUNT(*) AS lead_count FROM leads GROUP BY source ORDER BY lead_count DESC;`
  },
  {
    id: 126, difficulty: "Medium", company: "Snowflake", topic: "Window Functions",
    title: "Percent Rank of Query Execution Time",
    description: `You have a <code>query_log</code> table. For each query, show its <strong>execution time and percent rank</strong> within its warehouse.\n\nReturn: <code>query_id</code>, <code>warehouse</code>, <code>exec_ms</code>, <code>pct_rank</code>`,
    schema: `CREATE TABLE query_log (query_id INT, warehouse TEXT, exec_ms INT);
INSERT INTO query_log VALUES (1,'WH1',500),(2,'WH1',1200),(3,'WH1',800),(4,'WH1',300),(5,'WH2',1000),(6,'WH2',600),(7,'WH2',1500),(8,'WH2',200);`,
    hint: "PERCENT_RANK() or manual (RANK()-1)/(COUNT()-1)",
    solution: `SELECT query_id, warehouse, exec_ms, ROUND((RANK() OVER (PARTITION BY warehouse ORDER BY exec_ms)-1)*1.0/(COUNT(*) OVER (PARTITION BY warehouse)-1),4) AS pct_rank FROM query_log ORDER BY warehouse, exec_ms;`
  },
  {
    id: 127, difficulty: "Medium", company: "Instacart", topic: "Anti Join",
    title: "Items Never Reordered",
    description: `You have an <code>order_history</code> table. Find items that were only purchased <strong>exactly once</strong> (never reordered).\n\nReturn: <code>item_name</code>, <code>purchase_count</code>`,
    schema: `CREATE TABLE order_history (id INT, user_id INT, item_name TEXT, order_date TEXT);
INSERT INTO order_history VALUES (1,101,'Milk','2024-01-01'),(2,101,'Eggs','2024-01-01'),(3,102,'Milk','2024-01-05'),(4,101,'Milk','2024-01-10'),(5,102,'Bread','2024-01-08'),(6,103,'Eggs','2024-01-03');`,
    hint: "GROUP BY item_name HAVING COUNT(*) = 1",
    solution: `SELECT item_name, COUNT(*) AS purchase_count FROM order_history GROUP BY item_name HAVING COUNT(*) = 1 ORDER BY item_name;`
  },
  {
    id: 128, difficulty: "Medium", company: "Robinhood", topic: "Time Series",
    title: "Daily Net Portfolio Change",
    description: `You have a <code>portfolio_values</code> table. Calculate the <strong>day-over-day portfolio change</strong> for each account.\n\nReturn: <code>account_id</code>, <code>val_date</code>, <code>portfolio_value</code>, <code>daily_change</code>`,
    schema: `CREATE TABLE portfolio_values (id INT, account_id INT, val_date TEXT, portfolio_value DECIMAL);
INSERT INTO portfolio_values VALUES (1,501,'2024-01-01',10000),(2,501,'2024-01-02',10500),(3,501,'2024-01-03',10200),(4,501,'2024-01-04',11000),(5,502,'2024-01-01',5000),(6,502,'2024-01-02',4800),(7,502,'2024-01-03',5100);`,
    hint: "LAG(portfolio_value) OVER (PARTITION BY account_id ORDER BY val_date), then subtract",
    solution: `SELECT account_id, val_date, portfolio_value, portfolio_value - LAG(portfolio_value) OVER (PARTITION BY account_id ORDER BY val_date) AS daily_change FROM portfolio_values ORDER BY account_id, val_date;`
  },
  {
    id: 129, difficulty: "Medium", company: "Coinbase", topic: "Aggregation",
    title: "Total Trade Volume by Crypto Pair per Day",
    description: `You have a <code>trades</code> table. Calculate the <strong>total trading volume per crypto pair per day</strong>.\n\nReturn: <code>pair</code>, <code>trade_date</code>, <code>total_volume</code>`,
    schema: `CREATE TABLE crypto_trades (id INT, pair TEXT, amount DECIMAL, trade_time TEXT);
INSERT INTO crypto_trades VALUES (1,'BTC-USD',0.5,'2024-01-01 10:00'),(2,'BTC-USD',1.2,'2024-01-01 14:00'),(3,'ETH-USD',2.0,'2024-01-01 11:00'),(4,'ETH-USD',0.8,'2024-01-02 09:00'),(5,'BTC-USD',0.3,'2024-01-02 10:00'),(6,'ETH-USD',1.5,'2024-01-02 15:00');`,
    hint: "GROUP BY pair, DATE(trade_time)",
    solution: `SELECT pair, DATE(trade_time) AS trade_date, SUM(amount) AS total_volume FROM crypto_trades GROUP BY pair, DATE(trade_time) ORDER BY pair, trade_date;`
  },
  {
    id: 130, difficulty: "Medium", company: "Figma", topic: "Collaboration",
    title: "Files Edited by Multiple Users",
    description: `You have a <code>file_edits</code> table. Find files that were edited by <strong>more than 2 different users</strong>.\n\nReturn: <code>file_id</code>, <code>editor_count</code>`,
    schema: `CREATE TABLE file_edits (id INT, file_id TEXT, editor_id INT, edited_at TEXT);
INSERT INTO file_edits VALUES (1,'F1',101,'2024-01-01'),(2,'F1',102,'2024-01-02'),(3,'F1',103,'2024-01-03'),(4,'F2',101,'2024-01-01'),(5,'F2',102,'2024-01-04'),(6,'F3',101,'2024-01-02'),(7,'F3',104,'2024-01-03'),(8,'F3',105,'2024-01-04');`,
    hint: "COUNT(DISTINCT editor_id) per file, HAVING > 2",
    solution: `SELECT file_id, COUNT(DISTINCT editor_id) AS editor_count FROM file_edits GROUP BY file_id HAVING COUNT(DISTINCT editor_id) > 2 ORDER BY editor_count DESC;`
  },
  {
    id: 131, difficulty: "Hard", company: "Netflix", topic: "Content Analytics",
    title: "Binge Watchers (3+ Episodes in One Day)",
    description: `You have a <code>watch_history</code> table. Find users who watched <strong>3 or more episodes in a single day at least once</strong>.\n\nReturn: <code>user_id</code>, <code>binge_date</code>, <code>episodes_watched</code>`,
    schema: `CREATE TABLE watch_history (id INT, user_id INT, show_id INT, watch_date TEXT);
INSERT INTO watch_history VALUES (1,101,1,'2024-01-01'),(2,101,1,'2024-01-01'),(3,101,1,'2024-01-01'),(4,101,1,'2024-01-02'),(5,102,2,'2024-01-01'),(6,102,2,'2024-01-01'),(7,103,1,'2024-01-01'),(8,103,1,'2024-01-01'),(9,103,1,'2024-01-01'),(10,103,1,'2024-01-01');`,
    hint: "GROUP BY user_id, watch_date HAVING COUNT(*) >= 3",
    solution: `SELECT user_id, watch_date AS binge_date, COUNT(*) AS episodes_watched FROM watch_history GROUP BY user_id, watch_date HAVING COUNT(*) >= 3 ORDER BY episodes_watched DESC;`
  },
  {
    id: 132, difficulty: "Hard", company: "LinkedIn", topic: "Network Analysis",
    title: "Users Who Posted Every Week in January",
    description: `You have a <code>posts</code> table. Find users who made at least one post in <strong>every week of January 2024</strong> (4 weeks).\n\nReturn: <code>user_id</code>`,
    schema: `CREATE TABLE posts (post_id INT, user_id INT, post_date TEXT);
INSERT INTO posts VALUES (1,101,'2024-01-01'),(2,101,'2024-01-08'),(3,101,'2024-01-15'),(4,101,'2024-01-22'),(5,102,'2024-01-01'),(6,102,'2024-01-08'),(7,102,'2024-01-15'),(8,103,'2024-01-05'),(9,103,'2024-01-12'),(10,103,'2024-01-19'),(11,103,'2024-01-26');`,
    hint: "COUNT DISTINCT weeks per user, HAVING = 4. Use strftime('%W') for week number.",
    solution: `SELECT user_id FROM posts WHERE post_date BETWEEN '2024-01-01' AND '2024-01-31' GROUP BY user_id HAVING COUNT(DISTINCT WEEK(post_date)) = 4 ORDER BY user_id;`
  },
  {
    id: 133, difficulty: "Hard", company: "Stripe", topic: "Fraud Detection",
    title: "Merchants with Sudden Revenue Spike",
    description: `You have a <code>merchant_daily</code> table. Find merchants whose revenue on any given day is <strong>more than 3x their 7-day moving average</strong>.\n\nReturn: <code>merchant_id</code>, <code>revenue_date</code>, <code>revenue</code>, <code>moving_avg</code>`,
    schema: `CREATE TABLE merchant_daily (id INT, merchant_id TEXT, revenue_date TEXT, revenue INT);
INSERT INTO merchant_daily VALUES (1,'M1','2024-01-01',1000),(2,'M1','2024-01-02',1100),(3,'M1','2024-01-03',950),(4,'M1','2024-01-04',1050),(5,'M1','2024-01-05',1000),(6,'M1','2024-01-06',1100),(7,'M1','2024-01-07',4500),(8,'M2','2024-01-01',500),(9,'M2','2024-01-02',520),(10,'M2','2024-01-03',480);`,
    hint: "Compute 7-day moving avg in CTE, then filter where revenue > moving_avg * 3",
    solution: `WITH avgs AS (SELECT *, ROUND(AVG(revenue) OVER (PARTITION BY merchant_id ORDER BY revenue_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW),2) AS moving_avg FROM merchant_daily)
SELECT merchant_id, revenue_date, revenue, moving_avg FROM avgs WHERE revenue > moving_avg * 3 ORDER BY merchant_id, revenue_date;`
  },
  {
    id: 134, difficulty: "Hard", company: "Uber Eats", topic: "Complex Joins",
    title: "Restaurants With Consistent High Ratings",
    description: `You have a <code>restaurant_reviews</code> table. Find restaurants that have received <strong>5 or more reviews and every single review is rated 4.0 or above</strong>.\n\nReturn: <code>restaurant_id</code>, <code>review_count</code>, <code>avg_rating</code>`,
    schema: `CREATE TABLE restaurant_reviews (id INT, restaurant_id INT, rating DECIMAL);
INSERT INTO restaurant_reviews VALUES (1,1,4.5),(2,1,4.8),(3,1,4.2),(4,1,4.9),(5,1,4.7),(6,2,4.1),(7,2,3.8),(8,2,4.5),(9,2,4.6),(10,2,4.3),(11,3,4.8),(12,3,4.9),(13,3,5.0),(14,3,4.7),(15,3,4.8);`,
    hint: "GROUP BY restaurant, HAVING COUNT(*) >= 5 AND MIN(rating) >= 4.0",
    solution: `SELECT restaurant_id, COUNT(*) AS review_count, ROUND(AVG(rating),2) AS avg_rating FROM restaurant_reviews GROUP BY restaurant_id HAVING COUNT(*) >= 5 AND MIN(rating) >= 4.0 ORDER BY avg_rating DESC;`
  },
  {
    id: 135, difficulty: "Hard", company: "Datadog", topic: "Metrics",
    title: "Services with P99 Latency Above Threshold",
    description: `You have a <code>latency_logs</code> table. For each service, find whether its <strong>99th percentile latency exceeds 500ms</strong>.\n\nReturn: <code>service_name</code>, <code>p99_latency</code>, <code>breaches_threshold</code>`,
    schema: `CREATE TABLE latency_logs (id INT, service_name TEXT, latency_ms INT);
INSERT INTO latency_logs VALUES (1,'api',200),(2,'api',250),(3,'api',180),(4,'api',300),(5,'api',220),(6,'api',190),(7,'api',210),(8,'api',280),(9,'api',600),(10,'api',230),(11,'db',100),(12,'db',120),(13,'db',90),(14,'db',110),(15,'db',105),(16,'db',95),(17,'db',100),(18,'db',115),(19,'db',108),(20,'db',102);`,
    hint: "Use NTILE(100) or rank-based approach; P99 = value at 99th percentile. Alternatively use PERCENT_RANK to find value where percent_rank >= 0.99",
    solution: `WITH ranked AS (SELECT service_name, latency_ms, PERCENT_RANK() OVER (PARTITION BY service_name ORDER BY latency_ms) AS pct FROM latency_logs)
SELECT service_name, MIN(latency_ms) AS p99_latency,
  CASE WHEN MIN(latency_ms) > 500 THEN 'Yes' ELSE 'No' END AS breaches_threshold
FROM ranked WHERE pct >= 0.99 GROUP BY service_name ORDER BY service_name;`
  },
  {
    id: 136, difficulty: "Easy", company: "Walmart", topic: "Basic Aggregation",
    title: "Total Sales per Store",
    description: `You have a <code>store_sales</code> table. Find the <strong>total sales amount per store</strong>.\n\nReturn: <code>store_id</code>, <code>total_sales</code>`,
    schema: `CREATE TABLE store_sales (sale_id INT, store_id INT, amount DECIMAL);
INSERT INTO store_sales VALUES (1,'S1',250),(2,'S1',180),(3,'S2',300),(4,'S2',120),(5,'S3',500),(6,'S3',200),(7,'S3',150);`,
    hint: "GROUP BY store_id, SUM(amount)",
    solution: `SELECT store_id, SUM(amount) AS total_sales FROM store_sales GROUP BY store_id ORDER BY total_sales DESC;`
  },
  {
    id: 137, difficulty: "Easy", company: "Salesforce", topic: "Filtering",
    title: "Opportunities in Closing Stage",
    description: `You have an <code>opportunities</code> table. Return all opportunities in <strong>'Closing' or 'Closed Won'</strong> stage.\n\nReturn: <code>opp_id</code>, <code>account_name</code>, <code>stage</code>, <code>amount</code>`,
    schema: `CREATE TABLE opportunities (opp_id INT, account_name TEXT, stage TEXT, amount DECIMAL);
INSERT INTO opportunities VALUES (1,'Acme','Prospecting',5000),(2,'TechCorp','Closing',25000),(3,'BigCo','Closed Won',50000),(4,'StartupX','Proposal',10000),(5,'MegaCorp','Closing',75000),(6,'FinFirm','Closed Won',30000);`,
    hint: "WHERE stage IN ('Closing', 'Closed Won')",
    solution: `SELECT opp_id, account_name, stage, amount FROM opportunities WHERE stage IN ('Closing','Closed Won') ORDER BY amount DESC;`
  },
  {
    id: 138, difficulty: "Easy", company: "Square", topic: "Aggregation",
    title: "Average Transaction Value by Payment Method",
    description: `You have a <code>square_transactions</code> table. Calculate the <strong>average transaction value per payment method</strong>.\n\nReturn: <code>payment_method</code>, <code>avg_amount</code>, <code>transaction_count</code>`,
    schema: `CREATE TABLE square_transactions (id INT, payment_method TEXT, amount DECIMAL);
INSERT INTO square_transactions VALUES (1,'card',45.00),(2,'cash',12.50),(3,'card',89.99),(4,'card',23.00),(5,'contactless',55.00),(6,'cash',8.75),(7,'contactless',120.00),(8,'card',34.50);`,
    hint: "GROUP BY payment_method, AVG(amount), COUNT(*)",
    solution: `SELECT payment_method, ROUND(AVG(amount),2) AS avg_amount, COUNT(*) AS transaction_count FROM square_transactions GROUP BY payment_method ORDER BY avg_amount DESC;`
  },
  {
    id: 139, difficulty: "Medium", company: "Twitch", topic: "Engagement",
    title: "Streamers with Growing Viewership (Week-over-Week)",
    description: `You have a <code>stream_views</code> table. Find streamers whose viewership <strong>increased every week</strong> for 3 consecutive weeks.\n\nReturn: <code>streamer_id</code>`,
    schema: `CREATE TABLE stream_views (id INT, streamer_id INT, week TEXT, avg_viewers INT);
INSERT INTO stream_views VALUES (1,101,'W1',1000),(2,101,'W2',1200),(3,101,'W3',1500),(4,102,'W1',2000),(5,102,'W2',1800),(6,102,'W3',2200),(7,103,'W1',500),(8,103,'W2',600),(9,103,'W3',750);`,
    hint: "Use LAG to get previous week, check each pair, then find streamers where all pairs are increasing",
    solution: `WITH changes AS (
  SELECT streamer_id, week, avg_viewers,
    avg_viewers > LAG(avg_viewers) OVER (PARTITION BY streamer_id ORDER BY week) AS increased
  FROM stream_views
)
SELECT streamer_id FROM changes GROUP BY streamer_id HAVING MIN(increased) = 1 ORDER BY streamer_id;`
  },
  {
    id: 140, difficulty: "Medium", company: "Atlassian", topic: "Project Metrics",
    title: "Sprints with All Tickets Completed",
    description: `You have a <code>sprint_tickets</code> table. Find sprints where <strong>every single ticket was completed</strong>.\n\nReturn: <code>sprint_id</code>, <code>total_tickets</code>`,
    schema: `CREATE TABLE sprint_tickets (id INT, sprint_id TEXT, ticket_id INT, status TEXT);
INSERT INTO sprint_tickets VALUES (1,'S1',1,'done'),(2,'S1',2,'done'),(3,'S1',3,'done'),(4,'S2',4,'done'),(5,'S2',5,'in_progress'),(6,'S2',6,'done'),(7,'S3',7,'done'),(8,'S3',8,'done');`,
    hint: "HAVING SUM(CASE WHEN status != 'done' THEN 1 ELSE 0 END) = 0",
    solution: `SELECT sprint_id, COUNT(*) AS total_tickets FROM sprint_tickets GROUP BY sprint_id HAVING SUM(CASE WHEN status != 'done' THEN 1 ELSE 0 END) = 0 ORDER BY sprint_id;`
  },
  {
    id: 141, difficulty: "Medium", company: "Peloton", topic: "Activity Metrics",
    title: "Members with Increasing Weekly Workout Count",
    description: `You have a <code>workouts</code> table. Find members whose workout count <strong>increased from week 1 to week 2</strong>.\n\nReturn: <code>member_id</code>, <code>w1_workouts</code>, <code>w2_workouts</code>`,
    schema: `CREATE TABLE workouts (id INT, member_id INT, workout_date TEXT);
INSERT INTO workouts VALUES (1,201,'2024-01-01'),(2,201,'2024-01-03'),(3,201,'2024-01-08'),(4,201,'2024-01-10'),(5,201,'2024-01-12'),(6,202,'2024-01-02'),(7,202,'2024-01-09'),(8,203,'2024-01-01'),(9,203,'2024-01-02'),(10,203,'2024-01-03');`,
    hint: "COUNT workouts in W1 (Jan 1-7) and W2 (Jan 8-14) per member, then filter where W2 > W1",
    solution: `WITH weekly AS (
  SELECT member_id,
    SUM(CASE WHEN workout_date BETWEEN '2024-01-01' AND '2024-01-07' THEN 1 ELSE 0 END) AS w1,
    SUM(CASE WHEN workout_date BETWEEN '2024-01-08' AND '2024-01-14' THEN 1 ELSE 0 END) AS w2
  FROM workouts GROUP BY member_id
)
SELECT member_id, w1 AS w1_workouts, w2 AS w2_workouts FROM weekly WHERE w2 > w1 ORDER BY member_id;`
  },
  {
    id: 142, difficulty: "Hard", company: "Palantir", topic: "Complex Analytics",
    title: "Entities with Anomalous Attribute Changes",
    description: `You have an <code>entity_scores</code> table. Find entities where any score change between consecutive measurements is <strong>greater than 50 points</strong>.\n\nReturn: <code>entity_id</code>, <code>measured_at</code>, <code>score</code>, <code>prev_score</code>, <code>change</code>`,
    schema: `CREATE TABLE entity_scores (id INT, entity_id TEXT, measured_at TEXT, score INT);
INSERT INTO entity_scores VALUES (1,'E1','2024-01-01',100),(2,'E1','2024-01-02',105),(3,'E1','2024-01-03',170),(4,'E2','2024-01-01',200),(5,'E2','2024-01-02',195),(6,'E2','2024-01-03',140),(7,'E3','2024-01-01',50),(8,'E3','2024-01-02',55),(9,'E3','2024-01-03',60);`,
    hint: "LAG to get prev score, compute abs change, filter where ABS(change) > 50",
    solution: `WITH changes AS (SELECT entity_id, measured_at, score, LAG(score) OVER (PARTITION BY entity_id ORDER BY measured_at) AS prev_score FROM entity_scores)
SELECT entity_id, measured_at, score, prev_score, score-prev_score AS change FROM changes WHERE ABS(score-prev_score) > 50 ORDER BY entity_id, measured_at;`
  },
  {
    id: 143, difficulty: "Hard", company: "Databricks", topic: "Data Quality",
    title: "Tables with Duplicate Primary Keys",
    description: `You have a <code>data_records</code> table representing merged data. Find all <code>record_id</code> values that appear <strong>more than once</strong> (duplicates).\n\nReturn: <code>record_id</code>, <code>occurrence_count</code>`,
    schema: `CREATE TABLE data_records (record_id INT, source TEXT, value TEXT);
INSERT INTO data_records VALUES (1,'source_a','val1'),(1,'source_b','val1'),(2,'source_a','val2'),(3,'source_a','val3'),(3,'source_b','val3'),(4,'source_a','val4');`,
    hint: "GROUP BY record_id HAVING COUNT(*) > 1",
    solution: `SELECT record_id, COUNT(*) AS occurrence_count FROM data_records GROUP BY record_id HAVING COUNT(*) > 1 ORDER BY occurrence_count DESC;`
  },
  {
    id: 144, difficulty: "Hard", company: "Notion", topic: "User Engagement",
    title: "Pages With High Edit Frequency",
    description: `You have a <code>page_edits</code> table. Find pages edited more than <strong>3 times on any single day</strong>.\n\nReturn: <code>page_id</code>, <code>edit_date</code>, <code>edit_count</code>`,
    schema: `CREATE TABLE page_edits (id INT, page_id TEXT, editor_id INT, edited_at TEXT);
INSERT INTO page_edits VALUES (1,'P1',101,'2024-01-01 09:00'),(2,'P1',102,'2024-01-01 10:00'),(3,'P1',101,'2024-01-01 11:00'),(4,'P1',103,'2024-01-01 14:00'),(5,'P2',101,'2024-01-01 09:00'),(6,'P2',102,'2024-01-01 10:00'),(7,'P3',101,'2024-01-02 09:00'),(8,'P3',101,'2024-01-02 10:00'),(9,'P3',102,'2024-01-02 11:00'),(10,'P3',103,'2024-01-02 12:00'),(11,'P3',104,'2024-01-02 13:00');`,
    hint: "GROUP BY page_id, DATE(edited_at) HAVING COUNT(*) > 3",
    solution: `SELECT page_id, DATE(edited_at) AS edit_date, COUNT(*) AS edit_count FROM page_edits GROUP BY page_id, DATE(edited_at) HAVING COUNT(*) > 3 ORDER BY edit_count DESC;`
  },
  {
    id: 145, difficulty: "Hard", company: "Cloudflare", topic: "Network Metrics",
    title: "IPs Exceeding Request Rate Limit",
    description: `You have a <code>request_logs</code> table. Find IP addresses that made <strong>more than 100 requests in any 1-hour window</strong>.\n\nReturn: <code>ip_address</code>, <code>window_start</code>, <code>request_count</code>`,
    schema: `CREATE TABLE request_logs (id INT, ip_address TEXT, request_time TEXT);
INSERT INTO request_logs VALUES (1,'1.1.1.1','2024-01-01 10:00'),(2,'1.1.1.1','2024-01-01 10:30'),(3,'2.2.2.2','2024-01-01 10:00'),(4,'2.2.2.2','2024-01-01 10:15'),(5,'2.2.2.2','2024-01-01 10:30');`,
    hint: "For each request, count requests by same IP within the next 60 minutes; flag if count > 100. (With sample data the threshold is 2 to show results)",
    solution: `SELECT a.ip_address, a.request_time AS window_start, COUNT(*) AS request_count FROM request_logs a JOIN request_logs b ON a.ip_address=b.ip_address AND b.request_time BETWEEN a.request_time AND DATE_ADD(a.request_time, INTERVAL 1 HOUR) GROUP BY a.ip_address, a.request_time HAVING COUNT(*) > 2 ORDER BY request_count DESC;`
  },
  {
    id: 146, difficulty: "Easy", company: "Zoom", topic: "COUNT DISTINCT",
    title: "Unique Participants per Meeting",
    description: `You have a <code>meeting_participants</code> table. Count the <strong>unique participants per meeting</strong>.\n\nReturn: <code>meeting_id</code>, <code>unique_participants</code>`,
    schema: `CREATE TABLE meeting_participants (id INT, meeting_id INT, user_id INT);
INSERT INTO meeting_participants VALUES (1,1,101),(2,1,102),(3,1,101),(4,2,101),(5,2,103),(6,2,104),(7,3,102),(8,3,105);`,
    hint: "COUNT(DISTINCT user_id) per meeting",
    solution: `SELECT meeting_id, COUNT(DISTINCT user_id) AS unique_participants FROM meeting_participants GROUP BY meeting_id ORDER BY meeting_id;`
  },
  {
    id: 147, difficulty: "Easy", company: "Box", topic: "Aggregation",
    title: "Storage Used per User",
    description: `You have a <code>file_storage</code> table. Find the <strong>total storage used per user in MB</strong>.\n\nReturn: <code>user_id</code>, <code>total_mb</code>`,
    schema: `CREATE TABLE file_storage (file_id INT, user_id INT, size_mb DECIMAL);
INSERT INTO file_storage VALUES (1,101,150.5),(2,101,200.0),(3,102,50.0),(4,102,75.5),(5,102,100.0),(6,103,500.0);`,
    hint: "SUM(size_mb) GROUP BY user_id",
    solution: `SELECT user_id, ROUND(SUM(size_mb),2) AS total_mb FROM file_storage GROUP BY user_id ORDER BY total_mb DESC;`
  },
  {
    id: 148, difficulty: "Medium", company: "Snap", topic: "DAU/WAU",
    title: "Daily Snap Opens per User",
    description: `You have a <code>snap_opens</code> table. Count <strong>daily opens per user</strong> and flag users with more than 5 opens as 'Power User'.\n\nReturn: <code>user_id</code>, <code>open_date</code>, <code>opens</code>, <code>user_type</code>`,
    schema: `CREATE TABLE snap_opens (id INT, user_id INT, open_date TEXT);
INSERT INTO snap_opens VALUES (1,101,'2024-01-01'),(2,101,'2024-01-01'),(3,101,'2024-01-01'),(4,101,'2024-01-01'),(5,101,'2024-01-01'),(6,101,'2024-01-01'),(7,102,'2024-01-01'),(8,102,'2024-01-01'),(9,103,'2024-01-01'),(10,103,'2024-01-01'),(11,103,'2024-01-01');`,
    hint: "COUNT(*) per user per date, CASE WHEN opens > 5 THEN 'Power User'",
    solution: `SELECT user_id, open_date, COUNT(*) AS opens, CASE WHEN COUNT(*) > 5 THEN 'Power User' ELSE 'Regular' END AS user_type FROM snap_opens GROUP BY user_id, open_date ORDER BY opens DESC;`
  },
  {
    id: 149, difficulty: "Medium", company: "Discord", topic: "Message Analysis",
    title: "Most Active Channel per Server",
    description: `You have a <code>discord_messages</code> table. Find the <strong>most active channel (by message count) per server</strong>.\n\nReturn: <code>server_id</code>, <code>channel_id</code>, <code>message_count</code>`,
    schema: `CREATE TABLE discord_messages (id INT, server_id INT, channel_id INT, user_id INT, sent_at TEXT);
INSERT INTO discord_messages VALUES (1,1,1,101,'2024-01-01'),(2,1,1,102,'2024-01-01'),(3,1,2,101,'2024-01-01'),(4,1,2,103,'2024-01-02'),(5,1,2,104,'2024-01-02'),(6,2,3,101,'2024-01-01'),(7,2,3,102,'2024-01-02'),(8,2,4,103,'2024-01-01');`,
    hint: "COUNT per server+channel, then ROW_NUMBER OVER (PARTITION BY server_id ORDER BY count DESC), filter rn=1",
    solution: `SELECT server_id, channel_id, message_count FROM (SELECT server_id, channel_id, COUNT(*) AS message_count, ROW_NUMBER() OVER (PARTITION BY server_id ORDER BY COUNT(*) DESC) AS rn FROM discord_messages GROUP BY server_id, channel_id) WHERE rn=1 ORDER BY server_id;`
  },
  {
    id: 150, difficulty: "Hard", company: "GitHub", topic: "Code Activity",
    title: "Repos With Commits Every Day in a Week",
    description: `You have a <code>commits</code> table. Find repos that had at least one commit on <strong>every day from Jan 1–7, 2024</strong>.\n\nReturn: <code>repo_id</code>`,
    schema: `CREATE TABLE commits (id INT, repo_id TEXT, committer_id INT, commit_date TEXT);
INSERT INTO commits VALUES (1,'R1',101,'2024-01-01'),(2,'R1',102,'2024-01-02'),(3,'R1',101,'2024-01-03'),(4,'R1',103,'2024-01-04'),(5,'R1',101,'2024-01-05'),(6,'R1',102,'2024-01-06'),(7,'R1',101,'2024-01-07'),(8,'R2',101,'2024-01-01'),(9,'R2',101,'2024-01-02'),(10,'R2',101,'2024-01-03'),(11,'R3',101,'2024-01-01'),(12,'R3',101,'2024-01-03'),(13,'R3',101,'2024-01-05'),(14,'R3',101,'2024-01-07');`,
    hint: "COUNT DISTINCT commit_date WHERE date in range, HAVING = 7",
    solution: `SELECT repo_id FROM commits WHERE commit_date BETWEEN '2024-01-01' AND '2024-01-07' GROUP BY repo_id HAVING COUNT(DISTINCT commit_date) = 7 ORDER BY repo_id;`
  },
  {
    id: 151, difficulty: "Easy", company: "Hubspot", topic: "Basic",
    title: "Count Contacts by Country",
    description: `You have a <code>contacts</code> table. Count contacts <strong>per country</strong>.\n\nReturn: <code>country</code>, <code>contact_count</code>`,
    schema: `CREATE TABLE contacts (contact_id INT, name TEXT, country TEXT);
INSERT INTO contacts VALUES (1,'Alice','USA'),(2,'Bob','UK'),(3,'Carol','USA'),(4,'David','Canada'),(5,'Eve','USA'),(6,'Frank','UK'),(7,'Grace','Germany');`,
    hint: "GROUP BY country, COUNT(*)",
    solution: `SELECT country, COUNT(*) AS contact_count FROM contacts GROUP BY country ORDER BY contact_count DESC;`
  },
  {
    id: 152, difficulty: "Easy", company: "Zendesk", topic: "Filtering + Aggregation",
    title: "Average Resolution Time for High Priority Tickets",
    description: `You have a <code>support_tix</code> table. Find the <strong>average resolution time in hours for HIGH priority tickets</strong>.\n\nReturn: <code>avg_resolution_hours</code>`,
    schema: `CREATE TABLE support_tix (ticket_id INT, priority TEXT, created_at TEXT, resolved_at TEXT);
INSERT INTO support_tix VALUES (1,'high','2024-01-01 09:00','2024-01-01 11:00'),(2,'medium','2024-01-01 10:00','2024-01-01 15:00'),(3,'high','2024-01-02 08:00','2024-01-02 10:30'),(4,'low','2024-01-02 09:00','2024-01-03 09:00'),(5,'high','2024-01-03 11:00','2024-01-03 14:00');`,
    hint: "WHERE priority='high', AVG(DATEDIFF(resolved, created)*24)",
    solution: `SELECT ROUND(AVG(DATEDIFF(resolved_at, created_at)*24),2) AS avg_resolution_hours FROM support_tix WHERE priority='high';`
  },
  {
    id: 153, difficulty: "Medium", company: "Zendesk", topic: "Trend",
    title: "Ticket Volume Trend (7-Day Moving Avg)",
    description: `You have a <code>daily_tickets</code> table. Calculate a <strong>7-day moving average of ticket volume</strong>.\n\nReturn: <code>ticket_date</code>, <code>ticket_count</code>, <code>moving_avg_7d</code>`,
    schema: `CREATE TABLE daily_tickets (id INT, ticket_date TEXT, ticket_count INT);
INSERT INTO daily_tickets VALUES (1,'2024-01-01',45),(2,'2024-01-02',52),(3,'2024-01-03',38),(4,'2024-01-04',61),(5,'2024-01-05',49),(6,'2024-01-06',55),(7,'2024-01-07',42),(8,'2024-01-08',68),(9,'2024-01-09',51),(10,'2024-01-10',44);`,
    hint: "AVG OVER (ORDER BY date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW)",
    solution: `SELECT ticket_date, ticket_count, ROUND(AVG(ticket_count) OVER (ORDER BY ticket_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW),2) AS moving_avg_7d FROM daily_tickets ORDER BY ticket_date;`
  },
  {
    id: 154, difficulty: "Medium", company: "Workday", topic: "HR Analytics",
    title: "Headcount by Department Month-over-Month",
    description: `You have a <code>headcount_monthly</code> table. Show headcount and <strong>MoM change per department</strong>.\n\nReturn: <code>department</code>, <code>month</code>, <code>headcount</code>, <code>mom_change</code>`,
    schema: `CREATE TABLE headcount_monthly (id INT, department TEXT, month TEXT, headcount INT);
INSERT INTO headcount_monthly VALUES (1,'Eng','2024-01',50),(2,'Eng','2024-02',52),(3,'Eng','2024-03',55),(4,'Sales','2024-01',30),(5,'Sales','2024-02',28),(6,'Sales','2024-03',32);`,
    hint: "LAG(headcount) OVER (PARTITION BY department ORDER BY month)",
    solution: `SELECT department, month, headcount, headcount-LAG(headcount) OVER (PARTITION BY department ORDER BY month) AS mom_change FROM headcount_monthly ORDER BY department, month;`
  },
  {
    id: 155, difficulty: "Hard", company: "Workday", topic: "HR Complex",
    title: "Employees Who Got Promoted (Salary Jump > 20%)",
    description: `You have a <code>salary_history</code> table. Find employees who experienced a <strong>salary increase of more than 20%</strong> in any single change.\n\nReturn: <code>emp_id</code>, <code>change_date</code>, <code>old_salary</code>, <code>new_salary</code>, <code>pct_increase</code>`,
    schema: `CREATE TABLE salary_history (id INT, emp_id INT, salary INT, effective_date TEXT);
INSERT INTO salary_history VALUES (1,101,80000,'2023-01-01'),(2,101,85000,'2023-07-01'),(3,101,105000,'2024-01-01'),(4,102,60000,'2023-01-01'),(5,102,63000,'2024-01-01'),(6,103,90000,'2023-01-01'),(7,103,115000,'2024-01-01');`,
    hint: "LAG(salary) to get old salary, compute pct change, filter > 20",
    solution: `WITH changes AS (SELECT emp_id, effective_date, salary, LAG(salary) OVER (PARTITION BY emp_id ORDER BY effective_date) AS prev_salary FROM salary_history)
SELECT emp_id, effective_date AS change_date, prev_salary AS old_salary, salary AS new_salary,
  ROUND((salary-prev_salary)*100.0/prev_salary,2) AS pct_increase
FROM changes WHERE (salary-prev_salary)*1.0/prev_salary > 0.2 ORDER BY pct_increase DESC;`
  }
];