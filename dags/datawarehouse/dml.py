import logging

logger = logging.getLogger(__name__)
table = "yt_api"


def insert_rows(cursor, connection, schema, row):
    try:

        if schema == "staging":
            video_id = "video_id"
            insert_query = f"""
                INSERT INTO {schema}.{table}("Video_ID", "Video_Title", "Upload_Date", "Duration", "Video_Views", "Likes_Count", "Comments_Count") VALUES (%(video_id)s, %(title)s, %(publishedAt)s, %(duration)s, %(viewCount)s, %(likeCount)s, %(commentCount)s)"""
            
            cursor.execute(insert_query, row)

        else:
            video_id = "Video_ID"

            insert_query = f"""
                INSERT INTO {schema}.{table}("Video_ID", "Video_Title", "Upload_Date", "Duration", "Video_Type", "Video_Views", "Likes_Count", "Comments_Count") VALUES (%(Video_ID)s, %(Video_Title)s, %(Upload_Date)s, %(Duration)s, %(Video_Type)s, %(Video_Views)s, %(Likes_Count)s, %(Comments_Count)s)"""
            
            cursor.execute(insert_query, row)

            connection.commit()
            logger.info(f"Inserted video ID {row[video_id]} into {schema}.{table} successfully.")

    except Exception as e:
        logger.error(f"Error inserting video ID {row[video_id]} into {schema}.{table}: {e}")
        raise e
    

def update_rows(cursor, connection, schema, row):
    try:

        if schema == "staging":
            video_id = "video_id"
            upload_date = "publishedAt"
            video_title = "title"
            video_views = "viewCount"
            likes_count = "likeCount"
            comments_count = "commentCount"
        
        else:
            video_id = "Video_ID"
            upload_date = "Upload_Date"
            video_title = "Video_Title"
            video_views = "Video_Views"
            likes_count = "Likes_Count"
            comments_count = "Comments_Count"

        update_query = f"""
            UPDATE {schema}.{table}
            SET "Video_Title" = %({video_title})s,
                "Video_Views" = %({video_views})s,
                "Likes_Count" = %({likes_count})s,
                "Comments_Count" = %({comments_count})s
            WHERE "Video_ID" = %({video_id})s AND "Upload_Date" = %({upload_date})s;
            """
        
        cursor.execute(update_query, row)
        connection.commit()
        logger.info(f"Updated video ID {row[video_id]} in {schema}.{table} successfully.")
    
    except Exception as e:
        logger.error(f"Error updating video ID {row[video_id]} in {schema}.{table}: {e}")
        raise e
    

def delete_rows(cursor, connection, schema, video_ids):
    try:
        video_ids_str = f"""({', '.join(f"'{id}'" for id in video_ids)})"""
        delete_query = f"""
            DELETE FROM {schema}.{table}
            WHERE "Video_ID" IN {video_ids_str};
            """
        
        cursor.execute(delete_query)
        connection.commit()
        logger.info(f"Deleted video IDs {video_ids} from {schema}.{table} successfully.")

    except Exception as e:
        logger.error(f"Error deleting video IDs {video_ids} from {schema}.{table}: {e}")
        raise e