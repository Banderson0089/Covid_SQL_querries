
--Query 1
SELECT SUM(ROUTES) as Number_of_Routes, AreaAccess
FROM PortfolioProject..OSMP_Climbing_Formations$

GROUP by AreaAccess
ORDER by SUM(ROUTES) desc

--Query 2
SELECT FEATURE as Formation, ROUTES, X, Y, SeasonalClosure
FROM PortfolioProject..OSMP_Climbing_Formations$
WHERE FormationType <> 'Boulder'

--Query 3
SELECT COUNT(SeasonalClosure) as Closed_Formations,  AreaAccess
FROM PortfolioProject..OSMP_Climbing_Formations$
WHERE FormationType <> 'Boulder' AND SeasonalClosure = 'Y'
GROUP by AreaAccess
ORDER by Closed_Formations desc


--Query 4
SELECT FEATURE, AreaAccess, UseRating, SeasonalClosure
FROM PortfolioProject..OSMP_Climbing_Formations$
WHERE FormationType <> 'Boulder'
ORDER by UseRating desc

SELECT *
FROM PortfolioProject..OSMP_Climbing_Formations$
WHERE FormationType <> 'Boulder'
ORDER by UseRating desc