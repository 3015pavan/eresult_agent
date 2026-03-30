"""
Setup database password for acadextract connection
"""

import os
import sys
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

def setup_password():
    """Setup database password interactively"""
    print("🔐 Database Password Setup")
    print("=" * 40)
    
    print("Your acadextract database is accessible via pgAdmin.")
    print("Please enter the PostgreSQL password for user 'postgres':")
    
    try:
        import getpass
        password = getpass.getpass("PostgreSQL password for 'postgres' user: ")
    except ImportError:
        password = input("PostgreSQL password for 'postgres' user: ")
    
    if not password:
        print("❌ No password provided")
        return False
    
    # Update .env file
    env_path = ".env"
    with open(env_path, 'r') as f:
        lines = f.readlines()
    
    updated_lines = []
    for line in lines:
        if line.startswith('DB_PASSWORD='):
            updated_lines.append(f'DB_PASSWORD={password}\n')
        else:
            updated_lines.append(line)
    
    with open(env_path, 'w') as f:
        f.writelines(updated_lines)
    
    print("✅ Password updated in .env file")
    return True

def test_final_connection():
    """Test final connection"""
    print(f"\n🔌 Testing Final Connection")
    print("=" * 40)
    
    try:
        # Reset connection pool
        from src.common.database import reset_pool
        reset_pool()
        
        from src.common.database import get_connection
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT version()")
                version = cur.fetchone()
                print(f"✅ Connection successful!")
                print(f"📊 PostgreSQL: {version[0]}")
                
                # Check database
                cur.execute("SELECT current_database()")
                db_name = cur.fetchone()[0]
                print(f"🗄️  Connected to: {db_name}")
                
                # Check tables
                cur.execute("""
                    SELECT COUNT(*) FROM information_schema.tables 
                    WHERE table_schema = 'public'
                """)
                table_count = cur.fetchone()[0]
                print(f"📋 Tables: {table_count}")
                
                # Initialize
                from src.common.database import init_db
                inst_id = init_db()
                print(f"🏛️  Institution ID: {inst_id}")
                
                print(f"\n🎉 PERFECT! AcadExtract is connected to acadextract database!")
                print(f"🚀 Ready to store extraction data from any institution!")
                return True
                
    except Exception as e:
        print(f"❌ Connection failed: {e}")
        return False

def main():
    """Main function"""
    print("🚀 AcadExtract Database Password Setup")
    
    if setup_password():
        test_final_connection()
        
        print(f"\n🔄 Restart AcadExtract to apply changes:")
        print(f"   python -m uvicorn src.api.app:app --host 127.0.0.1 --port 8002 --reload")

if __name__ == "__main__":
    main()
