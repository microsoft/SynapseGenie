
We introduce Generic utility scripts to help auto detect all delta lake files in a Synapse workspace and then auto perform optimize and vacuum on those files. This ensures the performance and size of delta lake remain intact with regular usage.

![image](https://user-images.githubusercontent.com/45026856/211544923-ff6bdd49-5256-4e63-9f52-08c80db59999.png)

The Delta lake Auto maintain notebook can be directly uploaded to any Synapse workspace and run. 
It is recommended to setup a trigger to run it weekly during low usage periods . 
