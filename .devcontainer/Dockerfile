FROM ubuntu:22.04

# [Optional] Uncomment this section to install additional OS packages.
RUN apt-get update && export DEBIAN_FRONTEND=noninteractive \
    && apt-get -y install --no-install-recommends curl postgresql-client \ 
    graphviz python3-dev graphviz libgraphviz-dev pkg-config

# [Optional] Uncomment the next lines to use go get to install anything else you need
# USER vscode
# RUN go get -x <your-dependency-or-tool>
# USER root

# [Optional] Uncomment this line to install global node packages.
# RUN su vscode -c "source /usr/local/share/nvm/nvm.sh && npm install -g <your-package-here>" 2>&1

COPY atlasgo.sh /usr/local/bin/
RUN chmod +x /usr/local/bin/atlasgo.sh
RUN apt-get update && apt-get install -y ca-certificates
RUN /usr/local/bin/atlasgo.sh

