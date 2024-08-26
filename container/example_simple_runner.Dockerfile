FROM python:3.12.3-slim

RUN pip install pandas uniplot

# The secret sauce here is to have this at the end of a runner's dockerfile
RUN echo '#!/bin/bash\npython -c "$@"' > /entrypoint.sh
RUN chmod +x /entrypoint.sh
ENTRYPOINT ["/entrypoint.sh"]