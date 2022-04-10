--selecting PortfolioProject as default.

use PortfolioProject;

select * from INFORMATION_SCHEMA.TABLES;

--fetching data from both the tables.
	
SELECT * 
FROM CovidSDeaths;

SELECT * 
FROM CovidSVaccinations;


--selecting the data that we will be use

select location, date, total_cases,new_cases, 
total_deaths, population
from CovidSDeaths
order by 1,2;


--Looking at total cases vs total deaths 
--shows the likelyhood of dying in your country.

select location, date, total_cases, total_deaths,
(total_deaths/total_cases)*100 as DeathPercent, population
from CovidSDeaths
--where location = 'india'
order by 1,2;

--Looking at the total case vs population.
--shows the percentage of population got covid

select location, date, total_cases,population, 
(total_cases/population)*100 as PercentPopulationInfected
from CovidSDeaths
where continent is not NULL
--where location = 'india'
order by 1,2;

--Looking at the countries with highest infection rate compared to population.

select location, population, max(total_cases) as HighestInfectionCount, 
max((total_cases/population))*100 as PercentPopulationInfected
from CovidSDeaths
where continent is not NULL
--where location = 'india'
group by location, population
order by PercentPopulationInfected desc;

--showing countries with  highest death count per popuplation.

SELECT location, max(total_deaths) as TotalDeathCount
from CovidSDeaths 
where continent is not NULL
GROUP BY location
order by TotalDeathCount desc;

-- Let's break things down by continent.
 
 --showing continents with  highest death count per popuplation.

SELECT continent, max(total_deaths) as TotalDeathCount
from CovidSDeaths 
where continent is not NULL
GROUP BY continent
order by TotalDeathCount desc;

--Looking at numbers by date 

SELECT date, SUM(new_cases) as Total_cases,
SUM(cast(new_deaths as int))as Total_deaths, 
(SUM(cast(new_deaths as int)) /SUM(new_cases))*100 as DeathPercentage
from CovidSDeaths
where continent is not null
group by date
order by 1,2;

--Looking at Total Population vs vaccination

select * from
CovidSDeaths d JOIN CovidSVaccinations v
on d.location = v.location 
and d.date = v.date;

--finding the total vaccination for each location (window function used)

select d.continent,d.location, d.date,format(d.population,'#,###,###') as population,
format(convert(bigint, new_vaccinations),'#,###,###') as new_vaccinations,
format(
	sum(
		convert(bigint, new_vaccinations)
	) over (partition by d.location),
'#,###,###')as total_vaccination
from CovidSDeaths d JOIN CovidSVaccinations v
on d.location = v.location 
and d.date = v.date
where d.continent is not null and v.new_vaccinations is not null
order by 1,2,3;


