CREATE TABLE dbo.TwitterData
(
ID INT IDENTITY(1,1) PRIMARY KEY,
CreatedTime DATETIME,
UserName NVARCHAR(100),
Tweet NVARCHAR(300),
RetweetsCount INT,
Location NVARCHAR(30),
Place NVARCHAR(50)
)
GO

CREATE PROC dbo.Insert_Twitter_Data
(
	@tweetinfo NVARCHAR(MAX)

)
AS
BEGIN
	INSERT INTO dbo.TwitterData(userName,Tweet,CreatedTime,RetweetsCount,Location,Place)
	SELECT	username,
			text,
			CONVERT(DATE,substring(created_time,4,7) + ' ' +RIGHT(created_time,4),110),
			ISNULL(retweets_count,0) AS Retweets,
			location,
			place
	FROM OPENJSON(@tweetinfo)
	WITH
	(
		username NVARCHAR(50),
		text NVARCHAR(190),
		created_time NVARCHAR(30),
		retweets_count INT,
		location NVARCHAR(50),
		place NVARCHAR(50)
	)
END
GO