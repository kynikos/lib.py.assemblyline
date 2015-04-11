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

"""
This library provides the :py:class:`Factory` class, whose goal is to
provide a flexible way to manage a series of functions that are supposed to
manipulate data in an assembly-line-like way. This is particularly useful for
applications where such functions are plugins that can be added arbitrarily
through configuration files.

The input data is progressively processed by :py:class:`_Station` objects,
which pass their product objects onto the next configured stations in the line.
The stations together make the whole "factory", which is run by a predefined
number of "workers", which represent threads. Workers are designed to
automatically distribute to the stations that have data to be processed.

Author: Dario Giovannetti <dev@dariogiovannetti.net>

License: GPLv3

GitHub: https://www.github.com/kynikos/lib.py.assemblyline

**Note:** as it is clear by reading this page, the documentation is still in a
poor state. If you manage to understand how this library works and want to help
documenting it, you are welcome to fork the GitHub repository and request to
pull your improvements. Everything is written in docstrings in the only
python module of the package.

Also, if you have any questions, do not hesitate to write the author an email!

Module contents
===============
"""

import threading
import queue

# TODO: Add example in documentation
# TODO: Make sure exceptions in threads are raised and their traceback output
# TODO: Implement "combiners" to receive multiple inputs and feed proper
#       combinations to stations
# TODO: Add tests


class Factory:
    """
    The only class that must be instantiated, representing the whole factory.
    """
    def __init__(self, workersN, stationsdata):
        """
        Constructor: instantiate a :py:class:`Factory` object.

        :param int workersN: The maximum number of workers (threads) that work
            in the factory.
        :param dict stationsdata: A dictionary mapping the station names to
            a sequence with the 3 values for the `process`, `inputname` and
            `outputnames` parameters described in :py:meth:`_Station.__init__`.
        """
        self.workers = threading.Semaphore(value=workersN)
        self.started = False
        self.queue = queue.Queue()

        # TODO: Check inputs and outputs are unique and not conflicting with
        #       each other
        #       Also validate the values of the various configuration
        #       parameters
        self.begin_stations = []
        self.inputname_to_station = {}
        for sname in stationsdata:
            process, inputname, outputnames = stationsdata[sname]
            station = _Station(self.workers, self.queue,
                               self.inputname_to_station, process, inputname,
                               outputnames)
            if inputname:
                if inputname in self.inputname_to_station:
                    # TODO: Use a proper exception
                    raise UserWarning()
                self.inputname_to_station[inputname] = station
            else:
                self.begin_stations.append(station)

    def begin(self):
        """
        The method to run to start the assembly line.

        It can only be run once per :py:class:`Factory` instance.
        """
        if self.started:
            # TODO: Use a proper exception
            raise UserWarning()
        self.started = True

        self.workers.acquire()
        thread = threading.Thread(target=self._feed_queue)
        thread.start()

        return self._recurse_queue()

    def _feed_queue(self):
        for station in self.begin_stations:
            self.queue.put((station, None))
        self.workers.release()

    def _recurse_queue(self):
        # I could instead start all the stations simultaneously with a thread
        # each, but that would require having at least as many workers as the
        # number of stations
        # Even another alternative would be starting only one thread per item,
        # i.e. have the item processed by the various stations always in the
        # same thread: that would greatly simplify the code, but it would be
        # less flexible with stations that output multiple products
        try:
            station, item = self.queue.get(timeout=1)
        except queue.Empty:
            # Just checking len(self.queue) wouldn't be enough, because there
            # can be moments when the queue is empty because all the items are
            # being processed by some station
            if threading.active_count() > 1:
                return self._recurse_queue()
            else:
                # TODO: Store/return the final products
                return True
        self.workers.acquire()
        thread = threading.Thread(target=station.process_item, args=(item, ))
        # TODO: Name the thread with the task name + worker number
        thread.start()
        return self._recurse_queue()


class _Station:
    """
    The class that represents a station.
    """
    def __init__(self, workers, queue, inputname_to_station, process,
                 inputname, outputnames):
        """
        Constructor: instantiate a :py:class:`_Station` object.

        :param :py:class:`threading.Semaphore` workers: A reference to
            :py:attr:`Factory.workers`.
        :param :py:class:`queue.Queue` queue: A reference to
            :py:attr:`Factory.queue`.
        :param dict inputname_to_station: A reference to
            :py:attr:`Factory.inputname_to_station`.
        :param process: The instance of an object that represents the process
            to be executed in the station: it must at least implement a
            `process` generator that accepts 1 parameter (one item passed by
            the previous station) and yields a sequence (the products that
            will be passed to the other stations as configured)
        :param str inputname: The name of the input from which items will be
            retrieved.
        :param tuple outputnames: A sequence of output names that define which
            stations will process the products of this station.
        """
        self.workers = workers
        self.queue = queue
        self.inputname_to_station = inputname_to_station
        self.process = process
        self.inputname = inputname
        self.outputnames = outputnames

    def process_item(self, item):
        """
        The method to run to process an item.

        :param item: The object to process.
        """
        for products in self.process.process(item):
            for oi, oname in enumerate(self.outputnames):
                station = self.inputname_to_station[oname]
                self.queue.put((station, products[oi]))
        self.queue.task_done()
        self.workers.release()
