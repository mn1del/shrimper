# Authentication Overview

Shrimper uses first-party authentication for all admin features. This document summarises the moving parts and how to operate them in production.

## Accounts

- Admin records live in the `admins` table (`email`, `password_hash`, `is_superadmin`, `is_active`, timestamps).
- Passwords are hashed with Werkzeug’s PBKDF2 implementation.
- Super admins can manage other admins via `/admins`; regular admins can only edit races/fleet.
- Inactive admins cannot sign in and do not receive reset emails.

## Bootstrapping

Create the first account using the CLI:

```bash
flask --app app auth-create-admin --email you@example.com --superadmin
```

The command prompts for a password and prints a confirmation. Run it again to add more admins or to give someone super-admin privileges.

## Login & Sessions

- `SECRET_KEY` must be set for session signing.
- Successful logins update `last_login_at` for auditing.
- Super admins are required for schema/index management endpoints and the Admins UI.

## Password Resets

- Reset links are available from the login page or can be triggered by a super admin.
- Tokens are single-use, expire after one hour, and are stored hashed (`SHA-256`).
- Requests capture the IP address (`requested_ip`) for visibility.
- Emails are sent through Flask-Mail; configure SMTP credentials through the `MAIL_*` environment variables. Set `MAIL_SUPPRESS_SEND=1` in development to avoid sending.

## Admin Management UI

- `/admins` lists all admins and allows super admins to:
  - Add new admins (with optional immediate reset emails).
  - Disable/reactivate accounts.
  - Promote/demote super-admin privileges (cannot remove the last super admin).
  - Send one-click password resets.
- The UI calls JSON APIs under `/api/admins` which respond with the public admin payload (without password hashes).

## API summary

| Endpoint | Method | Description | Auth |
|----------|--------|-------------|------|
| `/api/admins` | GET | List admins | Super admin |
| `/api/admins` | POST | Create admin (`email`, `password`, `is_superadmin`, optional `send_reset_link`) | Super admin |
| `/api/admins/<id>` | PATCH | Update `is_active`, `is_superadmin`, or reset password | Super admin |
| `/api/admins/<id>/reset` | POST | Send reset email | Super admin |

## Environment variables

| Variable | Purpose |
|----------|---------|
| `SECRET_KEY` | Session signing key |
| `SECURITY_PASSWORD_SALT` | Salt used for password-reset tokens |
| `MAIL_SERVER`, `MAIL_PORT`, `MAIL_USERNAME`, `MAIL_PASSWORD` | SMTP credentials |
| `MAIL_USE_TLS`, `MAIL_USE_SSL`, `MAIL_DEFAULT_SENDER` | Mail transport options |
| `MAIL_SUPPRESS_SEND` | Set to `1` to suppress delivery (testing) |

Ensure these values are supplied in `.env` or your deployment environment before enabling authentication features.
