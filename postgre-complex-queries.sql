-- First query

CREATE VIEW rating_tag_view AS 
SELECT DISTINCT r.movieId, r.rating, t.tag, m.genres 
FROM ratings r INNER JOIN tags t ON r.userId=t.userId 
INNER JOIN movies m ON r.movieId=m.movieId 
GROUP BY r.movieId, r.userId, t.tag, r.rating, m.genres 
HAVING r.userId=121;

CREATE VIEW gt_gs_view AS 
SELECT DISTINCT gt.tag, gs.relevance 
FROM "genome-tags" gt INNER JOIN "genome-scores" gs ON gt.tagId=gs.tagId 
GROUP BY gt.tag, gs.relevance, gt.tagId;

CREATE VIEW relevant_view AS 
SELECT t.movieId, t.rating, t.tag, gt.relevance 
FROM gt_gs_view gt INNER JOIN rating_tag_view t ON t.tag = gt.tag 
GROUP BY t.movieId, t.genres, t.rating, t.tag, gt.relevance 
HAVING gt.relevance > 0.7;

SELECT DISTINCT m.movieId, m.title 
FROM relevant_view r INNER JOIN movies m ON r.movieId=m.movieId 
ORDER BY m.movieId;

-- Second query

SELECT DISTINCT m.movieId, m.title, AVG(r.rating) 
FROM movies m INNER JOIN ratings r ON m.movieId=r.movieId 
WHERE (length(m.title) - length(replace(m.title, 'Wars', ''))) > 0 
AND timestamp >= 1262304000 
AND timestamp <= 1451606399 
GROUP BY m.movieId;

-- Third query

CREATE VIEW reco AS 
SELECT r.movieId, m.genres, r.userId, r.rating, r.timestamp 
FROM ratings r INNER JOIN movies m ON m.movieId=r.movieId 
WHERE r.timestamp > 1356998400 AND r.userId = 69322 AND r.rating > 3 
ORDER BY r.timestamp DESC 
LIMIT 20;

SELECT DISTINCT m.title, m.genres 
FROM movies m INNER JOIN reco r ON split_part(m.genres, '|', 1)=split_part(r.genres, '|', 1) 
LIMIT 10;
