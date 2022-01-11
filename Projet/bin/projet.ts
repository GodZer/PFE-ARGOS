#!/usr/bin/env node
import * as cdk from 'aws-cdk-lib';
import { ProjetStack } from '../lib/projet-stack';

const app = new cdk.App();
new ProjetStack(app, 'ProjetStack');
