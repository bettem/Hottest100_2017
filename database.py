import pyodbc


class connection():
    def __init__(self):
        self.user = 'python'
        self.password = 'python'
        self.database = 'Hottest100'
        self.server = 'DESKTOP-B4P6BSN'

    def connect(self):
        self.cnxn = pyodbc.connect(
            'Driver={SQL Server};Server=' + self.server + ';database=' + self.database + ';uid=' + self.user + ';pwd=' + self.password + '')
        self.cursor = self.cnxn.cursor()

    def insert_processed_image(self, post_id):
        SQLCommand = ("INSERT INTO dbo.Processed_Images(Post_ID) VALUES(?)")
        Values = [post_id]
        self.cursor.execute(SQLCommand, Values)
        self.cnxn.commit()

    def insert_vote_results(self, post_id, artist_track):
        SQLCommand = ("INSERT INTO dbo.OCR_Results(Post_ID, Artist_Track_Name) VALUES (?,?)")
        Values = [post_id, artist_track]
        self.cursor.execute(SQLCommand, Values)
        self.cnxn.commit()

    def insert_match_results(self, post_id, OCR_Artist_Track_Name, Matched_Artist_Track_Name, Match_Likelihood):
        SQLCommand = (
            "INSERT INTO dbo.Processed_Votes(Post_ID, OCR_Artist_Track_Name, Matched_Artist_Track_Name, Match_Likelihood) VALUES (?,?,?,?)")
        Values = [post_id, OCR_Artist_Track_Name, Matched_Artist_Track_Name, Match_Likelihood]
        self.cursor.execute(SQLCommand, Values)
        self.cnxn.commit()

    def set_vote_processed(self):
        SQLCommand = (
            "UPDATE dbo.OCR_Results SET Processed_Flag = 1 WHERE Post_ID IN(SELECT Post_ID FROM dbo.Processed_Votes)")
        self.cursor.execute(SQLCommand)
        self.cnxn.commit()

    def get_song_list(self):
        rows = []
        SQLCommand = ("SELECT Artist_Track_Name FROM dbo.Song_List")
        self.cursor.execute(SQLCommand)
        while True:
            row = self.cursor.fetchone()
            if not row:
                break
            rows.append(row.Artist_Track_Name)
        return rows

    def get_processed_votes(self):
        rows = []
        SQLCommand = ("SELECT DISTINCT Post_ID FROM dbo.Processed_Images")
        self.cursor.execute(SQLCommand)
        while True:
            row = self.cursor.fetchone()
            if not row:
                break
            rows.append(row.Post_ID)
        return rows

    def get_raw_votes(self):
        rows = []
        SQLCommand = (
            "SELECT Post_ID, Artist_Track_Name FROM dbo.OCR_Results WHERE Artist_Track_Name <> '' AND Processed_Flag = 0")
        self.cursor.execute(SQLCommand)
        while True:
            row = self.cursor.fetchone()
            if not row:
                break
            rows.append([row.Post_ID, row.Artist_Track_Name])
        return rows

    def disconnect(self):
        self.cnxn.close()
