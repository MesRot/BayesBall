# Bayesball
Cleaned up repository for the BayesBall project. Note: All components are not included in this public project

### Model:
The model is a Bayesian model implemented in using Pymc3 inspired by https://bayesbet.everettsprojects.com/

##### Basic principles
- Each team has "offensive power" and "defensive power" but we are unsure what these are. Also the home team has inherent advantage against the away team and there is some constant value of goals that is scored on each game.
- More formally the goals are defined from the equation: $home_goals \sim Poisson(exp(home_team_offensive_power - away_team_defensive_power + home_team_advantage + constant_goal_value))$ and $away_goals \sim Poisson(exp(away_team_offensive_power - home_team_defensive_power + constant_goal_value))$

- First we have some guess on what these values might be. Lets say for all teams the offensive and defensive value is normally distributed with mean of 0 and standard deviation of 1. Same with the home team advantage.
- We calculate the posterior distribution for each team. For example let's say we had a game where FC Bayern Munich was playing against Borussia Dortmund and the game ended 3-0 for the FC Bayern Munich. Using this information the model updates the prior values.
- We might conclude that:
	- FC Bayern Munich had better offensive / defensive power
	- Borussia Dortmund had worse offensive / defensive power
	- Home team had advantage 
	- The constant value for goals is above 0.
- Using this information we might have:
	- FC Bayern Munich has offensive power with mean on 0.1 and standard deviation of 0.8
	- Borussia Dortmun has defensive power with mean on -0.1 and standard deviation of 0.8
	- Home team advantage is 0.6 with a standard deviation of 0.7
		- This means that home teams are likely to score ~1.8 (95% confidence intervals: [0.12, 7.94]) goals more than the away team
- When we have a new game day we use the previous posteriors as the priors except we add some uncertainty to these values.


### Components and insfracture:
- Data importing
	- TODO: add building and deploying to Makefile
    - Found in src/data_import
	- Includes
	  - Importing meta-information on all seasons to BayesBall ( src/data_import/fetch_seasons/ )
		- Clean 
		-
	  - Importing finished games to BayesBall ( src/data_import/fetch_teams_and_seasons/ )


