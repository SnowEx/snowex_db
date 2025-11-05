Amazon Web Services Configuration
===========================================================

These are instructions for deploying the snowex database on Amazon Web Services
(AWS). It involves creating a Relational Database System (RDS) Postgresql 
instance inside a private subnet. Within that subnet is a lambda function that 
has outbound rules allowing it to communicate with the RDS, and the RDS has 
inbound rules allowing it to receive communications from the lambda function.

There is a separate, public subnet containing a nat gateway. The lambda 
function is allowed to send outbound traffic to the nat gateway and the nat
gateway can then send information outbound to the internet gateway.

To take care of database administration and management we have set up and
EC2 instance inside the same private subnet as the RDS and Lambda function.
The EC2 has the IAM role AmazonSSMManagedInstanceCore attached. The EC2 has 
outbound rules to talk to the RDS as well as https via 443. We then use the
AWS Session Manager to create a secure connection to the RDS so that we can 
issue postgresql queries and ingest scripts via localhost. 

Administrators can connect in this way, only if they have local database
credentials, via this Linux terminal command shown below.

AWS RDS + Lambda + EC2 Management Architecture
----------------------------------------------

This document describes the system configuration for hosting 
a **PostgreSQL database (Amazon RDS)** inside a private subnet, 
providing **public API access** through AWS Lambda, and enabling 
**secure administration** using an **EC2 instance with Session Manager**.

Architecture Overview
---------------------

- **Amazon RDS (PostgreSQL)**
  - Deployed in a **private subnet**.
  - Only accessible within the VPC (not exposed publicly).

- **AWS Lambda**
  - Also deployed inside the private subnet.
  - Has outbound permissions to:
    - Connect to the RDS database.
    - Route internet-bound traffic via NAT Gateway for external API 
    interactions.

- **NAT Gateway**
  - Deployed in a **public subnet**.
  - Provides internet access for Lambda and EC2 instances in private subnets.

- **EC2 Instance (Bastion / Admin Host)**
  - Deployed in the same private subnet as RDS.
  - Equipped with the **AmazonSSMManagedInstanceCore** IAM role.
  - Used for database administration through **AWS Session Manager** port 
  forwarding.
  - Can connect securely to RDS without opening SSH ports.

Networking Configuration
------------------------

VPC
~~~

- **CIDR block**: e.g., ``10.0.0.0/16``
- **Subnets**:
  - **Private Subnet**: contains RDS, Lambda, EC2 (no direct internet access).
  - **Public Subnet**: contains NAT Gateway (attached to Internet Gateway).

Internet Gateway
~~~~~~~~~~~~~~~~
- Attached to the VPC for internet routing.

NAT Gateway
~~~~~~~~~~~
- Located in the **public subnet**.
- Allows **outbound internet traffic** for Lambda and EC2 instances in private 
subnets.

Security Groups & Rules
-----------------------

RDS Security Group
~~~~~~~~~~~~~~~~~~
- **Inbound**:
  - PostgreSQL (TCP 5432) from Lambda SG and EC2 SG.
- **Outbound**:
  - Default (allow all).

Lambda Security Group
~~~~~~~~~~~~~~~~~~~~~
- **Inbound**:
  - None (Lambda initiates outbound connections).
- **Outbound**:
  - PostgreSQL (TCP 5432) to RDS SG.
  - HTTPS (443) to internet via NAT Gateway.

EC2 Security Group
~~~~~~~~~~~~~~~~~~
- **Inbound**:
  - None required (Session Manager uses AWS API, not SSH).
- **Outbound**:
  - PostgreSQL (TCP 5432) to RDS SG.
  - HTTPS (443) for SSM agent and package updates.

EC2 Administration via Session Manager
--------------------------------------

IAM Role
~~~~~~~~
- **AmazonSSMManagedInstanceCore** attached to EC2.
- Grants access to use AWS Systems Manager Session Manager.

Session Manager Setup
~~~~~~~~~~~~~~~~~~~~~
Uses **AWS CLI** to establish a port-forwarded session from local machine to 
RDS via EC2.

Example command:
^^^^^^^^^^^^^^^^

.. code-block:: bash

    aws ssm start-session \
        --target i-0fe3a884e3b2fea58 \
        --document-name AWS-StartPortForwardingSessionToRemoteHost \
        --parameters '{"portNumber":["5432"],"localPortNumber":["5432"],"host":["snowexdb.crqqoae8anob.us-west-2.rds.amazonaws.com"]}'

System Diagram
--------------

.. mermaid::

    flowchart TB

    subgraph VPC["VPC"]
      subgraph Public_Subnet["Public Subnet"]
        IGW["Internet Gateway"]
        NAT["NAT Gateway"]
      end

      subgraph Private_Subnet["Private Subnet"]
        LAMBDA["Lambda Function"]
        RDS["Amazon RDS PostgreSQL"]
        EC2["EC2 Admin Host"]
      end
    end

    %% Connections
    LAMBDA -->|Query| RDS
    EC2 -->|Admin Access| RDS
    LAMBDA -->|Outbound Traffic| NAT
    NAT --> IGW
    EC2 -->|Outbound HTTPS| NAT

Sequence Diagram
----------------

.. mermaid::

    sequenceDiagram
        participant User as Public_API_User
        participant Lambda as AWS_Lambda_Private_Subnet
        participant RDS as Amazon_RDS_PostgreSQL
        participant Admin as Admin_Workstation
        participant EC2 as EC2_Admin_Host_Private_Subnet

        %% Public API request path
        User->>Lambda: Invoke API request
        Lambda->>RDS: Query database (5432)
        RDS-->>Lambda: Return query results
        Lambda-->>User: Return API response

        %% Admin access path
        Admin->>EC2: Start SSM Session (port forward 5432)
        EC2->>RDS: Connect to database
        RDS-->>EC2: Return query results
        EC2-->>Admin: Forward results via Session Manager
