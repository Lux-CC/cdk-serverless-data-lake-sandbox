from doctest import script_from_examples
from operator import is_
from constructs import Construct
from aws_cdk import (
    Duration,
    Stack,
    aws_iam as iam,
    aws_s3 as s3,
    aws_s3_deployment as s3deploy,
    aws_glue as glue,
    aws_athena as athena,
    RemovalPolicy,
    Size as awsSize,
    aws_sam as sam,
    aws_dynamodb as dynamodb,
    aws_ec2 as ec2,
    aws_rds as rds,
    aws_glue_alpha as glue_alpha
)

import os
from pathlib import Path

GLUE_DATABASE_NAME = "database"
S3_QUERY_DIRECTORY = "athena_queries"
DATA_FOLDER_S3_BUCKET = "raw"
SCRIPTS_FOLDER_S3_BUCKET = 'scripts'
root_dir = os.getcwd()
assets_dir_path = Path(root_dir) / "assets/"
scripts_dir_path = Path(root_dir) / "glue_scripts/scripts/"

ASSETS_FOLDER_LIST = os.listdir(assets_dir_path)

class DemoStack(Stack):
    demo_name: str
    vpc: ec2.Vpc
    dynamodb_table: dynamodb.Table
    raw_glue_db: glue_alpha.Database
    processed_glue_db: glue_alpha.Database
    glue_role: iam.Role
    raw_bucket: s3.Bucket
    glue_scripts_bucket: s3.Bucket
    processed_bucket: s3.Bucket
    def __init__(
        self, scope: Construct, construct_id: str, demo_name: str, **kwargs
    ) -> None:
        super().__init__(scope, construct_id, **kwargs)
        self.demo_name = demo_name
        self.vpc = None
        self.dynamodb_table = None
        
        self.raw_bucket = s3.Bucket(
            self,
            "demo_athena_bucket",
            removal_policy=RemovalPolicy.DESTROY,
            auto_delete_objects=True,
            bucket_name=self.name_resource("raw-data-bucket", is_global_resource=True),
        )
        self.processed_bucket = s3.Bucket(
            self,
            "demo_athena_bucket_processed",
            removal_policy=RemovalPolicy.DESTROY,
            auto_delete_objects=True,
            bucket_name=self.name_resource(
                "processed-data-bucket", is_global_resource=True
            ),
        )
        self.glue_scripts_bucket = s3.Bucket(
            self,
            "glue_scripts",
            removal_policy=RemovalPolicy.DESTROY,
            auto_delete_objects=True,
            bucket_name=self.name_resource("glue-scripts", is_global_resource=True),
        )
        # Deploy dataset to raw data bucket
        s3deploy.BucketDeployment(
            self,
            "demo_athena_bucket_deployment_populate_bucket_with_datasets",
            sources=[s3deploy.Source.asset(str(assets_dir_path))],
            destination_bucket=self.raw_bucket,
            destination_key_prefix=f"{DATA_FOLDER_S3_BUCKET}/",
            memory_limit=4096 * 2,
            ephemeral_storage_size=awsSize.mebibytes(4096 * 2),
        )
        # Deploy scripts to a more findable bucket than the cdk assets bucket
        s3deploy.BucketDeployment(
            self,
            "demo_gluescripts_bucket_deployment",
            sources=[s3deploy.Source.asset(str(scripts_dir_path))],
            destination_bucket=self.glue_scripts_bucket,
            destination_key_prefix=SCRIPTS_FOLDER_S3_BUCKET,
        )

        self.raw_glue_db = glue_alpha.Database(self, 'glue-default-raw-db',
            database_name=self.name_resource(f"{GLUE_DATABASE_NAME}_raw", delimiter="_").lower(),
        );

        self.processed_glue_db = glue_alpha.Database(self, 'glue-default-processed-db',
            database_name=self.name_resource(f"{GLUE_DATABASE_NAME}_processed", delimiter="_").lower(),
        );

        self.glue_role = iam.Role(
            self,
            "demoAthenaGlueRole",
            role_name=self.name_resource("glue-role", is_global_resource=True),
            assumed_by=iam.ServicePrincipal("glue.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name(
                    "service-role/AWSGlueServiceRole"
                )
            ],
            inline_policies={
                "demoAthenaS3BucketAccess": iam.PolicyDocument(
                    statements=[
                        iam.PolicyStatement(
                            actions=[
                                "s3:PutObject",
                                "s3:GetObject",
                                "s3:ListBucket",
                                "s3:DeleteObject",
                            ],
                            effect=iam.Effect.ALLOW,
                            resources=["*"],
                        )
                    ]
                )
            },
        )

        glue.CfnCrawler(
            self,
            "DemoAthenaCfnCrawlerWithoutClassifier",
            name=self.name_resource("crawler-without-classifier"),
            role=self.glue_role.role_arn,
            targets=glue.CfnCrawler.TargetsProperty(
                s3_targets=[
                    glue.CfnCrawler.S3TargetProperty(
                        path=f"{self.raw_bucket.s3_url_for_object()}/{DATA_FOLDER_S3_BUCKET}/{str(dataset_folder)}",
                    )
                    for dataset_folder in ASSETS_FOLDER_LIST
                ],
            ),
            database_name=self.raw_glue_db.database_name,
            table_prefix="",
        )

    def withAdminPrivileges(self):
        """Adds admin privileges to the glue role"""
        self.glue_role.add_managed_policy(iam.ManagedPolicy.from_aws_managed_policy_name(
                "AdministratorAccess"
            ))

    def withCustomClassifier(self):
        """Adds a classifier for string-only datasets.
        Classifier assumes there is a header row present, 
        which are normally not detected if all columns are strings
        """
        cfn_classifier = glue.CfnClassifier(
            self,
            "MyCfnClassifier",
            csv_classifier=glue.CfnClassifier.CsvClassifierProperty(
                allow_single_column=False,
                contains_header="PRESENT",
                delimiter="\t",
                disable_value_trimming=False,
                name=self.name_resource("contains-header-classifier"),
                quote_symbol="'",
            ),
        )

        glue.CfnCrawler(
            self,
            "DemoAthenaCfnCrawlerWithClassifier",
            role=self.glue_role.role_arn,
            name=self.name_resource("crawler-with-classifier"),
            targets=glue.CfnCrawler.TargetsProperty(
                s3_targets=[
                    glue.CfnCrawler.S3TargetProperty(
                        path=f"{self.raw_bucket.s3_url_for_object()}/{DATA_FOLDER_S3_BUCKET}/{str(dataset_folder)}",
                    )
                    for dataset_folder in ASSETS_FOLDER_LIST
                ],
            ),
            classifiers=[cfn_classifier.ref],
            database_name=self.raw_glue_db.database_name,
            table_prefix="with_classifier_",
        )

    def withGlueJobForNameBasics(self, path_to_file='name.basics/name.basics.tsv'):
        """Adds an example ETL job"""
        glue.CfnJob(self, 'glue-job-etl', 
            name=self.name_resource('s3-to-parquet'),
            description='Transform the name IMDB dataset to partitioned parquet',
            role=self.glue_role.role_arn,
            execution_property={'maxConcurrentRuns': 1},
            command={
                'name': 'glueetl',
                'pythonVersion': '3',
                'scriptLocation': self.glue_scripts_bucket.s3_url_for_object(f"{SCRIPTS_FOLDER_S3_BUCKET}/csv_to_parquet.py")
            },
            max_retries=1,
            timeout=20,
            number_of_workers=2,
            glue_version='3.0',
            worker_type='G.1X',
            default_arguments={
                '--job-language': 'python',
                '--enable-metrics': '',
                '--enable-continuous-cloudwatch-log': 'true',
                '--job-bookmark-option': 'job-bookmark-disable',
                '--raw_dataset_uri': f"{self.raw_bucket.s3_url_for_object()}/{DATA_FOLDER_S3_BUCKET}/{path_to_file}",
                '--target_dataset_uri': self.processed_bucket.s3_url_for_object(),
                '--dataset_path': path_to_file.split('/')[0]
            }
        )

    def withGlueV1DevEndpoint(self, ip_address_to_whitelist):
        # Note this is legacy and only works for glue v1
        if self.vpc:
            dev_ep_sg = ec2.SecurityGroup(self, 'endpoint-sg', vpc=self.vpc, allow_all_outbound=True)
            dev_ep_sg.add_ingress_rule(ec2.Peer.ipv4(ip_address_to_whitelist), ec2.Port(string_representation='22', from_port=22, to_port=22, protocol=ec2.Protocol.TCP))
        else:
            dev_ep_sg = None

        cfn_dev_endpoint = glue.CfnDevEndpoint(self, "MyCfnDevEndpoint",
            role_arn=self.glue_role,
            # arguments=,
            endpoint_name=self.name_resource('dev-endpoint'),
            # extra_jars_s3_path="extraJarsS3Path",
            # extra_python_libs_s3_path="extraPythonLibsS3Path",
            glue_version="1.0",
            number_of_workers=2,
            # public_keys=["publicKeys"],
            # security_configuration="securityConfiguration",
            worker_type="G.1X",
            **{
                'subnet_id': self.vpc.public_subnets[0].subnet_id,
                'security_group_ids': [dev_ep_sg.security_group_id]
            } if self.vpc else {},

        )

    def withAthena(self, include_federated_query_infra=True):
        """Adds infrastructure to support athena queries on the IMDB dataset"""
        query_results_s3_bucket = s3.Bucket(
            self,
            "athena_querie_results",
            removal_policy=RemovalPolicy.DESTROY,
            auto_delete_objects=True,
            bucket_name=self.name_resource(
                "athena-query-results", is_global_resource=True
            ),
        )

        athena_workgroup = athena.CfnWorkGroup(
            self,
            "demoAthenaWorkgroup",
            name=self.name_resource("athena_workgroup", delimiter="_"),
            recursive_delete_option=True,
            work_group_configuration=athena.CfnWorkGroup.WorkGroupConfigurationProperty(
                result_configuration=athena.CfnWorkGroup.ResultConfigurationProperty(
                    output_location=f"{query_results_s3_bucket.s3_url_for_object()}/{S3_QUERY_DIRECTORY}"
                )
            ),
        )

        athena_queries = [
            athena.CfnNamedQuery(
                self,
                f"demoAthenaQuery{dataset_folder}",
                database=self.raw_glue_db.database_name,
                query_string=f'SELECT * FROM "{self.raw_glue_db.database_name}"."with_classifier_{dataset_folder.replace(".", "_")}" LIMIT 10',
                description="Preview IMDB Dataset",
                name=self.name_resource(
                    f"preview_imdb_dataset_{dataset_folder}", delimiter="_"
                ),
                work_group=athena_workgroup.name,
            )
            for dataset_folder in ASSETS_FOLDER_LIST
        ]
        for query in athena_queries:
            query.add_depends_on(athena_workgroup)

        join_query = athena.CfnNamedQuery(
            self,
            f"demoAthenaJoin",
            database=self.raw_glue_db.database_name,
            query_string=f"SELECT with_classifier_title_basics.primarytitle, with_classifier_title_ratings.averagerating\nFROM with_classifier_title_ratings\nINNER JOIN with_classifier_title_basics ON with_classifier_title_ratings.tconst=with_classifier_title_basics.tconst",
            description="Joins two IMDB datasets",
            name=self.name_resource("join_imdb_datasets", delimiter="_"),
            work_group=athena_workgroup.name,
        )
        join_query.add_depends_on(athena_workgroup)

        if include_federated_query_infra:
            # BUcket for athena federated queries (requires a so-called 'spill' bucket for temp storage)
            athena_spill_bucket = s3.Bucket(
                self,
                "athena_spill_bucket",
                removal_policy=RemovalPolicy.DESTROY,
                auto_delete_objects=True,
                bucket_name=self.name_resource(
                    "athena-spill-bucket", is_global_resource=True
                ),
            )

            catalog_name = self.name_resource("dynamodb-catalog")
            dynamodb_connector = sam.CfnApplication(
                self,
                "DynamdobConnectorLambda",
                location={
                    "applicationId": "arn:aws:serverlessrepo:us-east-1:292517598671:applications/AthenaDynamoDBConnector",
                    "semanticVersion": "2022.22.1",
                },
                parameters={
                    "AthenaCatalogName": catalog_name,
                    "DisableSpillEncryption": "false",
                    "LambdaMemory": "3008",
                    "LambdaTimeout": "900",
                    "SpillBucket": athena_spill_bucket.bucket_name,
                    "SpillPrefix": "athena-spill",
                },
            )

            cfn_data_catalog = athena.CfnDataCatalog(
                self,
                "DynamodbFederatedQueryCatalog",
                name=self.name_resource("dynamodb"),
                type="LAMBDA",
                # the properties below are optional
                description="Uses federated query to query dynamodb",
                parameters={
                    "function": f"arn:aws:lambda:{self.region}:{self.account}:function:{catalog_name}"
                },
            )
            cfn_data_catalog.add_depends_on(dynamodb_connector)
            # Add a dynamodb table if not done so already
            if not self.dynamodb_table:
                self = self.with_dynamodb()

        return self

    def with_vpc(self):
        """Creates a VPC and makes sure the glue job runs inside a VPC"""
        self.vpc = ec2.Vpc(
            self,
            "vpc",
            cidr="10.0.0.0/16",
            nat_gateways=1,
            max_azs=3,
            subnet_configuration=[
                {
                    "cidrMask": 20,
                    "name": self.name_resource("public", delimiter="_"),
                    "subnetType": ec2.SubnetType.PUBLIC,
                },
                {
                    "cidrMask": 20,
                    "name": self.name_resource("private_app", delimiter="_"),
                    "subnetType": ec2.SubnetType.PRIVATE_WITH_NAT,
                },
                {
                    "cidrMask": 20,
                    "name": self.name_resource("private_db", delimiter="_"),
                    "subnetType": ec2.SubnetType.PRIVATE_ISOLATED,
                },
            ],
            gateway_endpoints={"S3": {"service": ec2.GatewayVpcEndpointAwsService.S3}},
        )
        return self

    def with_aurora(self):
        """Creates an aurora db. If no VPC is created yet, it adds the VPC as well."""
        if not self.vpc:
            self = self.with_vpc()

        cluster = rds.ServerlessCluster(
            self,
            "serverlessCluster",
            engine=rds.DatabaseClusterEngine.AURORA_MYSQL,
            credentials=rds.Credentials.from_generated_secret("clusterAdmin"),
            deletion_protection=False,
            cluster_identifier=self.name_resource("rds-cluster"),
            vpc=self.vpc,
            vpc_subnets={"subnet_type": ec2.SubnetType.PRIVATE_ISOLATED},
            scaling={
                "auto_pause": Duration.minutes(10),
                "min_capacity": rds.AuroraCapacityUnit.ACU_2,
                "max_capacity": rds.AuroraCapacityUnit.ACU_16,
            },
        )
        for subnet in self.vpc.private_subnets:
            cluster.connections.allow_default_port_from(ec2.Peer.ipv4(subnet.ipv4_cidr_block))
        return self

    def with_dynamodb(self):
        """Adds a dynamodb table with just a partition key called 'id'"""
        self.dyanamodb_table = dynamodb.Table(
            self,
            "test_table",
            table_name=self.name_resource("dynamodb-table"),
            partition_key=dynamodb.Attribute(
                name="id", type=dynamodb.AttributeType.STRING
            ),
            billing_mode=dynamodb.BillingMode.PAY_PER_REQUEST,
            removal_policy=RemovalPolicy.DESTROY,
        )
        self.dyanamodb_table.grant_read_write_data(self.glue_role);
        return self

    def name_resource(
        self, resource_name: str, is_global_resource=False, delimiter="-"
    ):
        """Just a method to name resources more consistently"""
        name_components = [
            item
            for item in [
                "demo",
                resource_name,
                self.account if is_global_resource else None,
                self.region if is_global_resource else None,
                self.demo_name,
            ]
            if item
        ]
        return delimiter.join(name_components)
