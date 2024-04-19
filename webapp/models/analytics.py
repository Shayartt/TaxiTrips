from typing import Dict
import pandas as pd 
# Second level import
from .db_connection import CassandraReader



class AnalyticsGenerator():
    """
    This class is responsible for gathering the data from Cassandra and generating the analytics.
    
    """
    def __init__(self, cassandra_handler: CassandraReader) -> None: # Take it as parameter instead of creating it to avoid creating multiple connection since this will be created inside the API.
        """
        Constructor will start the connection to Cassandra using signv4 plugins.
        
        """
        self._db_handler = cassandra_handler
        
    def load_dashboard_variables(self) -> Dict[str, str]:
        """
        Load the dashboard variables.
        
        Returns:
        data : Dict[str, str] : The dashboard variables.
        
        """
        
        res = {}
        
        # Load all data : 
        all_data = self._db_handler.read_from_cassandra("select * from traffic_db.streaming_record")
        df = pd.DataFrame(all_data)
        
        # Get new df with df['tpep_dropoff_datetime'] == today : 
        df['date'] = pd.to_datetime(df['tpep_dropoff_datetime']).dt.date
        today_df = df[df['date'] == pd.Timestamp.now().date()]
        history_df = df[df['date'] != pd.Timestamp.now().date()]
        
        count_analytics = self.load_total_counts(today_df, history_df)
        res.update(count_analytics)
        
        count_new_user = self.load_new_users(today_df, history_df)
        res.update(count_new_user)
        
        return res
    
    def load_total_counts(self, today_df: pd.DataFrame, history_df: pd.DataFrame) -> Dict[str, str]:
        """
        Load the total counts.
        
        :param today_df: pd.DataFrame : Today's dataframe.
        :param history_df: pd.DataFrame : History's dataframe.
        
        Returns:
        data : Dict[str, str] : The total counts.
        
        """
        
        
        return {
            "total_count": len(today_df['user_id'].unique()),
            "total_amount" : round(today_df['total_amount'].sum(), 2),
            "total_distance" : round(today_df['trip_distance'].sum(), 2)
        }
        
    def load_new_users(self, today_df: pd.DataFrame, history_df: pd.DataFrame) -> Dict[str, str]:
        """
        Load the new users.
        
        :param today_df: pd.DataFrame : Today's dataframe.
        :param history_df: pd.DataFrame : History's dataframe.
        
        Returns:
        data : Dict[str, str] : The new users.
        
        """
        
        
        # Get unique user_id and compare them with df, get only new ones : 
        today_users = today_df['user_id'].unique()
        new_users = [x for x in today_users if x not in history_df['user_id'].unique()]
         
        
        return {
            "new_users": len(new_users)
        }