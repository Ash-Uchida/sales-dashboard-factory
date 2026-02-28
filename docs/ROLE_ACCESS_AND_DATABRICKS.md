# Role access and Databricks setup

## Where passwords live and who can access them

- **Stored:** Passwords are stored only as **hashes** in `workspace.admin.users.password_hash` (SHA256). The app and `auth_view` never return the hash or plaintext to the UI.
- **Who can set passwords:** Users can self-register via Sign up on the login page. **IT Admin** can also create users and reset passwords via the **User management** page.
- **Who can see passwords:** Nobody. Plaintext passwords are never stored. Login only verifies the password (via `check_password` procedure or a hash comparison); the value is never exposed.
- **First IT Admin:** Create at least one IT Admin user directly in Databricks (e.g. INSERT into `workspace.admin.users` with `role_id = 'role_admin'` and a hashed password), or run a one-time script. After that, that user can log in and create others from User management.
- **Password reset:** IT Admin resets passwords via User management and shares the new password with the user manually.

---

## Role access matrix (app)

| Feature | Business User | Manager | Data Analyst | IT Admin |
|--------|----------------|---------|--------------|----------|
| **Dashboard** | ✅ | ✅ | ✅ | ✅ |
| **Chat (NL to SQL)** | ✅ (store-scoped, 1K rows) | ✅ (store-scoped, 1K rows) | ✅ (all tables, 100K) | ✅ (all tables, 100K) |
| **Add Transaction** | ✅ | ✅ | ✅ | ✅ |
| **Add Customer** | ✅ | ✅ | ✅ | ✅ |
| **Add Product** | ✅ | ✅ | ✅ | ✅ |
| **Add Store** | ❌ | ✅ | ❌ | ❌ |
| **Audit Log** | ❌ | ❌ | ✅ | ✅ |
| **User management** (list users, create user, set password) | ❌ | ❌ | ❌ | ✅ |

- **Chat / SQL:** Business User and Manager can only query `transactions`, scoped to their store, max 1,000 rows. Data Analyst and IT Admin can query `transactions`, `customers`, `products`, max 100,000 rows.
- **User management** (IT Admin only) lets admins create users and reset passwords. Self-registration is also available via Sign up on the login page.

---

## What to change in Databricks

### 1. Roles and first user

- Ensure `workspace.admin.roles` has the four roles (see `governance/admin_schema.sql`). If Manager was added later, run:
  ```sql
  INSERT INTO workspace.admin.roles (role_id, role_name) VALUES ('role_manager', 'Manager');
  ```
- Create at least one IT Admin user (so they can use User management to create others). Example (replace password with your choice; hash is SHA256 hex):
  ```sql
  -- Example: create first IT Admin (password: changeme)
  INSERT INTO workspace.admin.users (user_id, firstname, lastname, username, password_hash, role_id, store_id, created_at)
  VALUES (
    'admin-001',
    'Admin',
    'User',
    'admin',
    sha2('changeme', 256),
    'role_admin',
    NULL,
    current_timestamp()
  );
  ```
  Note: In Databricks SQL, `sha2('changeme', 256)` returns a hex string; if your app uses a different hashing format, generate the hash in the app and paste it here.

### 2. Optional: restrict who can read/write admin tables (Unity Catalog)

For defense-in-depth, you can limit which principals can access admin objects:

- **auth_view (read):** Grant SELECT to the role(s) your app uses to connect (e.g. a service principal or “app” role). All logged-in users are validated via this view; the app connection needs to read it.
- **users (read/write):** Restrict SELECT/INSERT/UPDATE so only IT Admin–like roles (or the app’s service principal when acting for User management) can read or modify. That way only authorized identities can create users or change password hashes.
- **stores (read):** Grant SELECT to roles that need to list stores (dashboard, Add Transaction, Add Customer, Add Store dropdown). **INSERT** on `workspace.admin.stores` can be restricted to a “Manager” or “IT Admin” role so only they can add stores, even from another client.

### 3. Optional: restrict sales tables by role

- **workspace.sales.transactions / customers / products:** You can grant SELECT to “Business User” and “Manager” only on tables (or a view) they’re allowed to see, and grant MODIFY only to roles that should insert (e.g. the app’s principal when running Add Transaction / Add Customer / Add Product). Data Analyst and IT Admin can have broader SELECT if desired.

### 4. Summary of app vs Databricks

| Control | Where | Purpose |
|--------|--------|--------|
| Who can open User management / create users / set password | **App (code)** | Only IT Admin sees the page and can submit. |
| Who can open Add Store | **App (code)** | Only Manager. |
| Who can open Audit Log | **App (code)** | Only Data Analyst and IT Admin. |
| Who can query which tables in Chat | **App (code)** | Validator uses role to allow tables and limits. |
| Who can read/write admin.users, admin.stores, sales.* | **Databricks (Unity Catalog)** | Optional; use grants to enforce at the data layer. |

Implementing the table and view permissions above in Databricks is optional but recommended so that even if someone bypasses the app, data and user/password management stay protected.
