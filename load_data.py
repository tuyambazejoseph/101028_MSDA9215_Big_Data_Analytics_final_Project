"""
================================================================================
MONGODB DATA LOADER
================================================================================
AUCA Big Data Analytics - Final Project

PURPOSE:
This script loads the generated JSON data into MongoDB.
After running this, you can see all data in MongoDB Compass!

HOW TO RUN:
1. Make sure MongoDB is running
2. Open PowerShell
3. cd C:\ecommerce_project\mongodb
4. python load_data.py

WHAT IT DOES:
1. Connects to MongoDB on your computer
2. Creates a database called "ecommerce_analytics"
3. Loads users, products, categories, transactions, and sessions
4. Creates indexes for faster queries

================================================================================
"""

# ============================================================================
# STEP 1: IMPORT LIBRARIES
# ============================================================================
print("=" * 60)
print("MONGODB DATA LOADER")
print("=" * 60)

import json
import os
import glob
from datetime import datetime

# Try to import pymongo
try:
    from pymongo import MongoClient
    print("✓ pymongo library loaded")
except ImportError:
    print("✗ pymongo not installed. Installing now...")
    os.system("pip install pymongo")
    from pymongo import MongoClient
    print("✓ pymongo installed and loaded")

# ============================================================================
# STEP 2: CONFIGURATION
# ============================================================================
# Path to your data folder
DATA_DIR = r"C:\ecommerce_project\data"

# MongoDB connection string (default local installation)
MONGO_URI = "mongodb://localhost:27017/"

# Database name
DATABASE_NAME = "ecommerce_analytics"

# ============================================================================
# STEP 3: CONNECT TO MONGODB
# ============================================================================
print("\n[1/6] Connecting to MongoDB...")

try:
    client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
    # Test connection
    client.server_info()
    print(f"  ✓ Connected to MongoDB at {MONGO_URI}")
except Exception as e:
    print(f"  ✗ ERROR: Could not connect to MongoDB!")
    print(f"    Make sure MongoDB is running.")
    print(f"    Error details: {e}")
    print("\n  TRY THIS:")
    print("  1. Open MongoDB Compass")
    print("  2. Click 'Connect' to start the server")
    print("  3. Run this script again")
    exit(1)

# Get database
db = client[DATABASE_NAME]
print(f"  ✓ Using database: {DATABASE_NAME}")

# ============================================================================
# STEP 4: HELPER FUNCTION TO LOAD JSON
# ============================================================================
def load_json_file(filepath):
    """Load a JSON file and return its contents."""
    print(f"    Loading {os.path.basename(filepath)}...", end=" ")
    with open(filepath, 'r', encoding='utf-8') as f:
        data = json.load(f)
    print(f"({len(data):,} records)")
    return data

# ============================================================================
# STEP 5: LOAD CATEGORIES
# ============================================================================
print("\n[2/6] Loading Categories...")

categories_file = os.path.join(DATA_DIR, "categories.json")
if os.path.exists(categories_file):
    categories = load_json_file(categories_file)
    
    # Drop existing collection and insert new data
    db.categories.drop()
    db.categories.insert_many(categories)
    print(f"  ✓ Loaded {len(categories)} categories into MongoDB")
else:
    print(f"  ✗ ERROR: {categories_file} not found!")

# ============================================================================
# STEP 6: LOAD PRODUCTS
# ============================================================================
print("\n[3/6] Loading Products...")

products_file = os.path.join(DATA_DIR, "products.json")
if os.path.exists(products_file):
    products = load_json_file(products_file)
    
    db.products.drop()
    db.products.insert_many(products)
    print(f"  ✓ Loaded {len(products):,} products into MongoDB")
else:
    print(f"  ✗ ERROR: {products_file} not found!")

# ============================================================================
# STEP 7: LOAD USERS
# ============================================================================
print("\n[4/6] Loading Users...")

users_file = os.path.join(DATA_DIR, "users.json")
if os.path.exists(users_file):
    users = load_json_file(users_file)
    
    db.users.drop()
    db.users.insert_many(users)
    print(f"  ✓ Loaded {len(users):,} users into MongoDB")
else:
    print(f"  ✗ ERROR: {users_file} not found!")

# ============================================================================
# STEP 8: LOAD TRANSACTIONS
# ============================================================================
print("\n[5/6] Loading Transactions...")

transactions_file = os.path.join(DATA_DIR, "transactions.json")
if os.path.exists(transactions_file):
    transactions = load_json_file(transactions_file)
    
    db.transactions.drop()
    
    # Insert in batches (500K records is large)
    BATCH_SIZE = 50000
    total = len(transactions)
    
    for i in range(0, total, BATCH_SIZE):
        batch = transactions[i:i+BATCH_SIZE]
        db.transactions.insert_many(batch)
        progress = min(i + BATCH_SIZE, total)
        print(f"    Inserted {progress:,}/{total:,} transactions...")
    
    print(f"  ✓ Loaded {len(transactions):,} transactions into MongoDB")
else:
    print(f"  ✗ ERROR: {transactions_file} not found!")

# ============================================================================
# STEP 9: LOAD SESSIONS (Multiple files)
# ============================================================================
print("\n[6/6] Loading Sessions...")
print("  (This may take several minutes due to 2 million records...)")

# Find all session files
session_files = sorted(glob.glob(os.path.join(DATA_DIR, "sessions_*.json")))

if session_files:
    db.sessions.drop()
    
    total_sessions = 0
    for session_file in session_files:
        sessions = load_json_file(session_file)
        
        # Insert in batches
        BATCH_SIZE = 50000
        for i in range(0, len(sessions), BATCH_SIZE):
            batch = sessions[i:i+BATCH_SIZE]
            db.sessions.insert_many(batch)
        
        total_sessions += len(sessions)
        print(f"    Total loaded so far: {total_sessions:,} sessions")
    
    print(f"  ✓ Loaded {total_sessions:,} sessions into MongoDB")
else:
    print(f"  ✗ ERROR: No session files found in {DATA_DIR}")

# ============================================================================
# STEP 10: CREATE INDEXES
# ============================================================================
print("\n[BONUS] Creating indexes for faster queries...")

# Indexes make queries MUCH faster
# Think of them like the index at the back of a book

# Products indexes
db.products.create_index("product_id")
db.products.create_index("category_id")
db.products.create_index("is_active")
print("  ✓ Created indexes on products collection")

# Users indexes
db.users.create_index("user_id")
db.users.create_index("geo_data.state")
print("  ✓ Created indexes on users collection")

# Transactions indexes
db.transactions.create_index("transaction_id")
db.transactions.create_index("user_id")
db.transactions.create_index("session_id")
db.transactions.create_index("timestamp")
print("  ✓ Created indexes on transactions collection")

# Sessions indexes
db.sessions.create_index("session_id")
db.sessions.create_index("user_id")
db.sessions.create_index("start_time")
db.sessions.create_index("conversion_status")
print("  ✓ Created indexes on sessions collection")

# ============================================================================
# STEP 11: VERIFY DATA
# ============================================================================
print("\n" + "=" * 60)
print("DATA LOADING COMPLETE!")
print("=" * 60)

print("\nCollection Summary:")
print("-" * 40)
print(f"  categories:   {db.categories.count_documents({}):>12,} documents")
print(f"  products:     {db.products.count_documents({}):>12,} documents")
print(f"  users:        {db.users.count_documents({}):>12,} documents")
print(f"  transactions: {db.transactions.count_documents({}):>12,} documents")
print(f"  sessions:     {db.sessions.count_documents({}):>12,} documents")
print("-" * 40)

# Quick stats
total_revenue = list(db.transactions.aggregate([
    {"$group": {"_id": None, "total": {"$sum": "$total"}}}
]))
if total_revenue:
    print(f"\nTotal Revenue: ${total_revenue[0]['total']:,.2f}")

# Close connection
client.close()

print("\n" + "=" * 60)
print("NEXT STEPS:")
print("=" * 60)
print("""
1. Open MongoDB Compass
2. Connect to mongodb://localhost:27017
3. Click on 'ecommerce_analytics' database
4. You should see 5 collections:
   - categories (25 documents)
   - products (5,000 documents)
   - users (10,000 documents)
   - transactions (500,000 documents)
   - sessions (2,000,000 documents)

5. Click on any collection to explore the data!
""")
