#!/usr/bin/env node
import * as cdk from 'aws-cdk-lib';
import { CodeStack } from '../lib/code-stack';

const app = new cdk.App();
new CodeStack(app, 'CodeStack');
