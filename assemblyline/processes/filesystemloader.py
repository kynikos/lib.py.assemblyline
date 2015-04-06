# AssemblyLine - Flexible management of threaded worker functions.
# Copyright (C) 2015 Dario Giovannetti <dev@dariogiovannetti.net>
#
# This file is part of AssemblyLine.
#
# AssemblyLine is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# AssemblyLine is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with AssemblyLine.  If not, see <http://www.gnu.org/licenses/>.

import os


class Process:
    def __init__(self):
        pass

    def process(self, item):
        for fname in os.listdir(item):
            if os.path.isfile(os.path.join(item, fname)):
                yield (fname, )
