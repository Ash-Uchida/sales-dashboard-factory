# Sales Dashboard Factory — User Guide for Business Users

This guide helps business users get started with the Sales Dashboard app.

---

## Logging In

1. Open the app in your browser.
2. Enter your **username** and **password**.
3. If you don’t have an account, click **Sign up** to create one (or ask your IT Admin to create one for you).
4. After login, you’ll see the dashboard and sidebar.

---

## Dashboard

The main dashboard shows:

- **KPIs:** Revenue, Orders, Customers, Average Order Value, Units Sold
- **Charts:** Revenue trend, Revenue by store, Product mix
- **Filters:** Use **Start Date**, **End Date**, and **Store** to narrow the data.

Your view is scoped to your store (if you’re a Business User or Manager). Data Analyst and IT Admin can see all stores.

---

## Asking Questions (Chat)

1. Open the **Chat** area (or use the chat interface on the dashboard).
2. Type a question in plain English, for example:
   - “What was revenue by store last month?”
   - “Top 5 products by revenue”
   - “How many orders did we have?”
3. The app turns your question into SQL, validates it, and shows the result as text and charts.
4. You can only ask about approved sales data. Some questions may be blocked if they exceed your access.

---

## Adding Data

Depending on your role, you can add:

- **Add Transaction:** New sales orders (order date, store, product, quantity, customer)
- **Add Customer:** New customers (customer ID, name, segment, store)
- **Add Product:** New products (product ID, name, price, category)

**Managers** can also **Add Store** to create new stores in the system.

---

## Roles and Access

| Role          | Dashboard | Chat | Add Transaction/Customer/Product | Add Store | Audit Log | User Management |
|---------------|-----------|------|-----------------------------------|-----------|-----------|-----------------|
| Business User | ✅        | ✅   | ✅                                | ❌        | ❌        | ❌              |
| Manager       | ✅        | ✅   | ✅                                | ✅        | ❌        | ❌              |
| Data Analyst  | ✅        | ✅   | ✅                                | ❌        | ✅        | ❌              |
| IT Admin      | ✅        | ✅   | ✅                                | ❌        | ✅        | ✅              |

- **Audit Log:** Only Data Analyst and IT Admin can view query logs.
- **User management (create user, reset password):** Only IT Admin.
- **Add Store:** Only Manager.

---

## Forgot Password?

Contact your **IT Admin**. They can reset your password from the User management page. If your email is on file and the app is configured for email, you’ll receive the new password by email.

---

## Tips

- Use clear, business-focused questions in chat (e.g. “revenue by store”, “top products”).
- Adjust the date range and store filter to focus on the period and location you care about.
- If a question is blocked, it may be outside your role’s access. Ask your Data Analyst or IT Admin for help.
