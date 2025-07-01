# 🕒 Automated Timesheet Integration Script

## 📌 Overview

This Python script automates the process of synchronizing employee timesheet data between **Compliance Genie** and **Sage HR**.

By fetching and posting attendance records programmatically, it ensures timesheet consistency, accuracy, and eliminates repetitive manual data entry.

---

## ⚙️ Technologies Used

- **Python**
- **REST APIs**
- **CSV Processing**
- **Multi-threading (concurrent.futures)**
- **Pandas**
- **Sage HR API**
- **Compliance Genie API**

---

## 🚀 Features

- 🔄 **Fetches** check-in/check-out data from Compliance Genie (CSV-based API).
- 🧹 **Cleans and formats** clock-in records to match Sage HR standards.
- ⏱ **Rounds time** to the nearest 5 minutes and applies intelligent break logic.
- 👥 **Matches employees** across systems using their names.
- 🧠 **Handles multiple shift entries** per employee per day.
- 🚀 **Posts data concurrently** to Sage HR API for faster processing.
- 🧾 **Tracks the last synced date** to avoid duplicate uploads.

---

## 📂 Folder Structure

```
your-project/
│
├── script.py               # Main integration script
├── .env                    # Contains sensitive API keys (not committed)
├── last_date.txt           # Tracks last processed date
├── requirements.txt        # List of dependencies
└── README.md               # Project documentation
```

---

## 🔐 Environment Variables

Use a `.env` file to store credentials and keys:

```
API_KEY=your_sage_hr_api_key
```

Load them in your script using:

```python
from dotenv import load_dotenv
load_dotenv()
```

---

## ✅ Prerequisites

- Python 3.7+
- `requests`, `pandas`, `python-dotenv`

Install dependencies:

```bash
pip install -r requirements.txt
```

---

## 🏁 Getting Started

1. Clone the repo
2. Add your `.env` file
3. Run the script:

```bash
python main.py
```

---

## 📌 Notes

- This script is intended for internal HR use.
- Be sure to test in a staging environment before running in production.
- Always back up `.txt` state files (`last_date.txt`) before resets.

---

## 📧 Contact

For issues or feature requests, please contact [Jhon Loyd Pastorin] at [jhonloydpastorin.03@gmail.com].
