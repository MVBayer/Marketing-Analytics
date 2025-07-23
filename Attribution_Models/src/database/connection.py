from sqlalchemy import create_engine
from contextlib import contextmanager
from sqlalchemy.orm import sessionmaker
import os
from pathlib import Path
import time

class DatabaseConnection:
    def __init__(self, connection_string=None):
        if connection_string is None:
            # Create data directory if it doesn't exist
            db_dir = Path('data')
            db_dir.mkdir(exist_ok=True)
            
            # Use file-based SQLite by default
            db_path = db_dir / 'attribution.db'
            # Add timeout and better connection handling
            connection_string = f"sqlite:///{db_path}?timeout=30"
        
        # Enable SQLite write-ahead logging for better concurrency
        self.engine = create_engine(
            connection_string,
            connect_args={'timeout': 30},  # 30 second timeout
            pool_pre_ping=True,  # Check connection before using
            pool_recycle=3600    # Recycle connections after 1 hour
        )
        self.Session = sessionmaker(bind=self.engine)
    
    @contextmanager
    def get_session(self):
        """Get a session for database operations with retry logic."""
        session = self.Session()
        max_retries = 3
        retry_count = 0
        
        while True:
            try:
                yield session
                # If we get here, the operation was successful
                session.commit()
                break
            except Exception as e:
                # Roll back on error
                session.rollback()
                retry_count += 1
                
                if retry_count >= max_retries:
                    print(f"Database error after {max_retries} retries: {str(e)}")
                    raise  # Re-raise the exception after max retries
                
                print(f"Database operation failed, retrying ({retry_count}/{max_retries}): {str(e)}")
                time.sleep(1)  # Wait before retrying
            finally:
                if retry_count >= max_retries or session.is_active:
                    # Only close the session if we're done retrying or if it's still active
                    session.close()
                

    
    def init_db(self):
        """Initialize database schema."""
        from .schema import metadata
        metadata.create_all(self.engine)
        
