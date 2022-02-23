
# Welcome on our CDK Python project!

You should explore the contents of this project. It demonstrates the CDK app with an instance of a stack (`ARGOS_STACK`) 

This project is set up like a standard Python project.  The initialization process also creates
a virtualenv within this project, stored under the .venv directory.  To create the virtualenv
it assumes that there is a `python3` executable in your path with access to the `venv` package.
If for any reason the automatic creation of the virtualenv fails, you can create the virtualenv
manually once the init process completes.

First, make sure you are in the correct folder:

```
$ cd App/
```

To manually create a virtualenv on MacOS and Linux:

```
$ python -m venv .venv
```

After the init process completes and the virtualenv is created, you can use the following
step to activate a virtualenv.

```
$ source .venv/bin/activate
```

If you are a Windows platform, you would activate a virtualenv like this:

```
% .venv\Scripts\activate.bat
```

Once the virtualenv is activated, you can install the required dependencies.

```
$ pip install -r requirements.txt
```

If necessary, specify to your interpreter the following path for the virtualenv:

```
./App/.venv/bin/python
```

Before carry on, don't forget to keep a Docker daemon up on your environment.

When you first use the project, you need to bootstrap the environment before deploying the project, please run:

```
$ cdk bootstrap
```

Now you can deploy the project by using the following command:

```
$ cdk deploy
```

Enjoy!

For more information on the project, please refer to the [Welcome Page](https://github.com/GodZer/PFE-ARGOS)

## Useful commands

 * `cdk ls`          list all stacks in the app
 * `cdk synth`       emits the synthesized CloudFormation template
 * `cdk deploy`      deploy this stack to your default AWS account/region
 * `cdk diff`        compare deployed stack with current state
 * `cdk docs`        open CDK documentation