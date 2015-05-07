README

This is the Flink implementation for DDF SPI.

New devs
---------
1. To have everything setup, run DDF/bin/run-once.sh
2. Import your projects into your IDE (The above script puts eclipse projects already. For Idea you can just directly open it as an SBt project)
3. A project for Flink will already be created. It has most dependencies set. If you need additional deps, look at the RootBuild.scala common deps to see if such deps are already there.
4. You can try out ddf-shell in the bin directory after doing a run-once.sh as all the deps will be available to the shell. If you have modified or added classes to the Flink project run 'mvn package' to make them available to the shell.
5. Maven poms for sub projects in DDF are generated files. You should only dependencies to the RootBuild.scala. The poms needs to be generated incase you add new deps by running bin/make-poms.sh
6. For SBT, run SBT from DDF(base dir) folder. Then choose the flink project by doing 'project flink'. You can do most things using SBT except for packaging/distribution and making classes available to the shell.
7. There is a style folder in the base DDF directory. Import the code formatter and check style into your respective IDEs. This is important. Please do not check in without applying the code formatter.




