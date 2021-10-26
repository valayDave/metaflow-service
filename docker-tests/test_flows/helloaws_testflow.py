from metaflow import FlowSpec, step, Parameter

class MetaflowFinalTestFlow(FlowSpec):
    """
    A flow where Metaflow prints 'Metaflow says Hi from AWS!'

    Run this flow to validate your AWS configuration.

    """
    # Apparently `--cpu` is not a good cli arg. 
    # Loudly fails on step-functions with `/bin/sh: 1: export: METAFLOW_INIT_--CPU: bad variable name`
    num_cpus = Parameter('num_cpu',default=10,help='Number of CPUS')
    num_gpus = Parameter('num_gpu',default=0,help='Number of GPUS')
    
    @step
    def start(self):
        """
        The 'start' step is a regular step, so runs locally on the machine from
        which the flow is executed.

        """
        from metaflow import get_metadata

        print("HelloAWS is starting.")
        print("")
        print("Using metadata provider: %s" % get_metadata())
        print("Something something something")
        self.next(self.hello,)

    @step
    def hello(self):
        """
        This steps runs remotely on AWS batch using 1 virtual CPU and 500Mb of
        memory. Since we are now using a remote metadata service and data
        store, the flow information and artifacts are available from
        anywhere. The step also uses the retry decorator, so that if something
        goes wrong, the step will be automatically retried.

        """
        self.message = 'Hi from AWS!'
        self.some_other_value = 10
        self.next(self.end)

    @step
    def end(self):
        """
        The 'end' step is a regular step, so runs locally on the machine from
        which the flow is executed.

        """
        print("MetaflowTestFlow is finished.")


if __name__ == '__main__':
    MetaflowFinalTestFlow()
