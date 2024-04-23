from typing import Dict
import pandas as pd 
# Second level import
from .db_connection import CassandraReader, OpenSearchReader



class AnalyticsGenerator():
    """
    This class is responsible for gathering the data from Cassandra and generating the analytics.
    
    """
    def __init__(self, cassandra_reader: CassandraReader, os_reader: OpenSearchReader) -> None: # Take it as parameter instead of creating it to avoid creating multiple connection since this will be created inside the API.
        """
        Constructor will start the connection to Cassandra using signv4 plugins.
        
        """
        self._cassandra_reader = cassandra_reader
        self._os_reader = os_reader
        
    def load_dashboard_variables(self) -> Dict[str, str]:
        """
        Load the dashboard variables.
        
        Returns:
        data : Dict[str, str] : The dashboard variables.
        
        """
        
        res = {}
        
        # Load all data : 
        all_data = self._cassandra_reader.read_from_cassandra("select * from traffic_db.streaming_record")
        df = pd.DataFrame(all_data)
        
        # Alerting : since this is a learning project, we will try to use both Cassandra and OpenSearch, normally in a production project it's totally fine to use only one of them.
        # Loading alerts :
        df_alerts = self._os_reader.read_from_opensearch("traffic_processing_tracker", {
                                "query": {
                                    "bool": {
                                    "filter": {
                                        "exists": {
                                        "field": "status"
                                        }
                                    }
                                    }
                                }
                                }
                                )
        
        # Get new df with df['tpep_dropoff_datetime'] == today : 
        df['date'] = pd.to_datetime(df['tpep_dropoff_datetime']).dt.date
        today_df = df[df['date'] == pd.Timestamp.now().date()]
        history_df = df[df['date'] != pd.Timestamp.now().date()]
        
        count_analytics = self.load_total_counts(today_df, history_df)
        res.update(count_analytics)
        
        count_new_user = self.load_new_users(today_df, history_df)
        res.update(count_new_user)
        
        all_today_trips = self.load_all_trips(today_df)
        res.update(all_today_trips)
        
        all_alerts_triggered = self.load_alerts_triggered(df_alerts)
        res.update(all_alerts_triggered)
        
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
        
    def load_all_trips(self, today_df: pd.DataFrame) -> Dict[str, str]:
        """
        This function will load all today's trip and prepare the format 
        
        :param today_df: pd.DataFrame : Today's dataframe.
        
        Returns:
        data : Dict[str, str] : The new users.
        
        """
        
        res = {
            "all_trips_count": today_df.shape[0]
        }
        all_trips_details = []
        
        for index,row in today_df.iterrows():
            all_trips_details.append({
                "user_id": row['user_id'],
                "fare_amount": round(row['fare_amount'],2),
                "tip_amount" : round(row['tip_amount'],2),
                "trip_duration" : row['trip_duration'],
                "count_passenger": row['passenger_count'],
                "destination": row['dolocationname']
            })
            
        res['all_trips_details'] = all_trips_details
        
        return res
    
    def load_alerts_triggered(self, df_alerts: pd.DataFrame) -> Dict[str, str]:
        """
        This function will load all the alerts triggered with details.
        

        """
        res = []
        for index,row in df_alerts.iterrows() : 
            res.append({
                "alert_message": row['details'],
                "trigger_date": row['datetime']
            })
        
        return {
                "alerts_details" : res
                }