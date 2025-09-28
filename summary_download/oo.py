#一个invoices的输入测试例子，确认接口没问题了就可以删除了


invoices = [
  {
    "buyer": "zj",
"invoice_date":"2025-09-01",
"category":"taxi",
"file_url": "https://xhavjxkrhsvezosozwma.supabase.co/storage/v1/object/sign/receiptDrop/save/9ad2a6a3-6a04-4842-9d74-3dcd0a8761b5/2025-09-28/2025-09-28T14%3A32%3A10.144543_Justificante_Pago_Tasa_Modelo790052.pdf?token=eyJraWQiOiJzdG9yYWdlLXVybC1zaWduaW5nLWtleV8zOTY2Njc0NS1iZjgxLTRlZWYtODk2Zi1jNmM0YzlkMDNjNmMiLCJhbGciOiJIUzI1NiJ9.eyJ1cmwiOiJyZWNlaXB0RHJvcC9zYXZlLzlhZDJhNmEzLTZhMDQtNDg0Mi05ZDc0LTNkY2QwYTg3NjFiNS8yMDI1LTA5LTI4LzIwMjUtMDktMjhUMTQ6MzI6MTAuMTQ0NTQzX0p1c3RpZmljYW50ZV9QYWdvX1Rhc2FfTW9kZWxvNzkwMDUyLnBkZiIsImlhdCI6MTc1OTA3MDUxMSwiZXhwIjoxNzU5MTU2OTExfQ.uxwpGyF4veFUqa-pPwSdp_3KQ7QGA-rCD64dL89toOQ",
"seller":"uber",
"invoice_total":36,
"currency":"EUR"

  },
  {
    "buyer": "zj",
"invoice_date":"2024-12-07",
"category":"train",
"file_url": "https://xhavjxkrhsvezosozwma.supabase.co/storage/v1/object/sign/receiptDrop/save/9ad2a6a3-6a04-4842-9d74-3dcd0a8761b5/2025-09-28/2025-09-28T14%3A32%3A10.144543_Justificante_Pago_Tasa_Modelo790052.pdf?token=eyJraWQiOiJzdG9yYWdlLXVybC1zaWduaW5nLWtleV8zOTY2Njc0NS1iZjgxLTRlZWYtODk2Zi1jNmM0YzlkMDNjNmMiLCJhbGciOiJIUzI1NiJ9.eyJ1cmwiOiJyZWNlaXB0RHJvcC9zYXZlLzlhZDJhNmEzLTZhMDQtNDg0Mi05ZDc0LTNkY2QwYTg3NjFiNS8yMDI1LTA5LTI4LzIwMjUtMDktMjhUMTQ6MzI6MTAuMTQ0NTQzX0p1c3RpZmljYW50ZV9QYWdvX1Rhc2FfTW9kZWxvNzkwMDUyLnBkZiIsImlhdCI6MTc1OTA3MDUxMSwiZXhwIjoxNzU5MTU2OTExfQ.uxwpGyF4veFUqa-pPwSdp_3KQ7QGA-rCD64dL89toOQ",
"seller":"OCA",
"invoice_total":79,
"currency":"EUR"

  },
{
    "buyer":"ssh",
"invoice_date":"2025-09-02",
"category":"train",
"file_url":"https://xhavjxkrhsvezosozwma.supabase.co/storage/v1/object/sign/receiptDrop/save/9ad2a6a3-6a04-4842-9d74-3dcd0a8761b5/2025-09-28/2025-09-28T14%3A25%3A41.904983_Receipt-2867-5007-5424.pdf?token=eyJraWQiOiJzdG9yYWdlLXVybC1zaWduaW5nLWtleV8zOTY2Njc0NS1iZjgxLTRlZWYtODk2Zi1jNmM0YzlkMDNjNmMiLCJhbGciOiJIUzI1NiJ9.eyJ1cmwiOiJyZWNlaXB0RHJvcC9zYXZlLzlhZDJhNmEzLTZhMDQtNDg0Mi05ZDc0LTNkY2QwYTg3NjFiNS8yMDI1LTA5LTI4LzIwMjUtMDktMjhUMTQ6MjU6NDEuOTA0OTgzX1JlY2VpcHQtMjg2Ny01MDA3LTU0MjQucGRmIiwiaWF0IjoxNzU5MDcwNTExLCJleHAiOjE3NTkxNTY5MTF9.5t2AUFBm73sZbgwZg5OLhi3A930s0Ek5-gpHUsZMY4Q",
"seller":"didi",
"invoice_total":125,
"currency":"CNY"

  },
{
    "buyer":"zj",
"invoice_date":"2025-09-01",
"category":"food",
"file_url": "https://xhavjxkrhsvezosozwma.supabase.co/storage/v1/object/sign/receiptDrop/save/9ad2a6a3-6a04-4842-9d74-3dcd0a8761b5/2025-09-28/2025-09-28T14%3A25%3A41.411043_Invoice-IIMTN1KG-0003.pdf?token=eyJraWQiOiJzdG9yYWdlLXVybC1zaWduaW5nLWtleV8zOTY2Njc0NS1iZjgxLTRlZWYtODk2Zi1jNmM0YzlkMDNjNmMiLCJhbGciOiJIUzI1NiJ9.eyJ1cmwiOiJyZWNlaXB0RHJvcC9zYXZlLzlhZDJhNmEzLTZhMDQtNDg0Mi05ZDc0LTNkY2QwYTg3NjFiNS8yMDI1LTA5LTI4LzIwMjUtMDktMjhUMTQ6MjU6NDEuNDExMDQzX0ludm9pY2UtSUlNVE4xS0ctMDAwMy5wZGYiLCJpYXQiOjE3NTkwNzA1MTEsImV4cCI6MTc1OTE1NjkxMX0.HDOSfSsJGUhZl7NKkcwZvaY9t9_qYN5E59XIQf6uwMw",
"seller":"shi tang",
"invoice_total":50,
"currency":"EUR"

  }
]