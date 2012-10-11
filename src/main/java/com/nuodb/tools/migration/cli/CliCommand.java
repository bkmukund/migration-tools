/**
 * Copyright (c) 2012, NuoDB, Inc.
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 *     * Redistributions of source code must retain the above copyright
 *       notice, this list of conditions and the following disclaimer.
 *     * Redistributions in binary form must reproduce the above copyright
 *       notice, this list of conditions and the following disclaimer in the
 *       documentation and/or other materials provided with the distribution.
 *     * Neither the name of NuoDB, Inc. nor the names of its contributors may
 *       be used to endorse or promote products derived from this software
 *       without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
 * WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 * DISCLAIMED. IN NO EVENT SHALL NUODB, INC. BE LIABLE FOR ANY DIRECT, INDIRECT,
 * INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA,
 * OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF
 * LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE
 * OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF
 * ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */
package com.nuodb.tools.migration.cli;

import com.nuodb.tools.migration.cli.parse.CommandLine;
import com.nuodb.tools.migration.cli.parse.OptionException;
import com.nuodb.tools.migration.cli.parse.Trigger;
import com.nuodb.tools.migration.cli.parse.option.ArgumentImpl;
import com.nuodb.tools.migration.cli.parse.option.OptionToolkit;
import com.nuodb.tools.migration.cli.parse.option.TriggerImpl;
import com.nuodb.tools.migration.cli.run.CliRun;
import com.nuodb.tools.migration.cli.run.CliRunFactory;
import com.nuodb.tools.migration.cli.run.CliRunFactoryLookup;

import java.util.HashSet;
import java.util.ListIterator;
import java.util.Set;

/**
 * @author Sergey Bushik
 */
public class CliCommand extends ArgumentImpl {

    private CliRunFactoryLookup runFactoryLookup;
    private OptionToolkit optionToolkit;

    public CliCommand(int id, String name, String description, boolean required,
                      CliRunFactoryLookup runFactoryLookup, OptionToolkit optionToolkit) {
        super(id, name, description, required);
        this.runFactoryLookup = runFactoryLookup;
        this.optionToolkit = optionToolkit;
    }

    @Override
    public Set<Trigger> getTriggers() {
        Set<Trigger> triggers = new HashSet<Trigger>();
        for (String command : runFactoryLookup.getCommands()) {
            triggers.add(new TriggerImpl(command));
        }
        return triggers;
    }

    @Override
    public boolean canProcess(CommandLine commandLine, String argument) {
        return runFactoryLookup.lookup(argument) != null;
    }

    @Override
    public void process(CommandLine commandLine, ListIterator<String> arguments) {
        CliRunFactory cliRunFactory = runFactoryLookup.lookup(arguments.next());
        CliRun cliRun = cliRunFactory.createCliRun(optionToolkit);
        cliRun.process(commandLine, arguments);
        commandLine.addValue(this, cliRun);
    }

    @Override
    public void validate(CommandLine commandLine) {
        CliRun command = commandLine.getValue(this);
        try {
            if (command != null) {
                command.validate(commandLine);
            }
        } catch (OptionException exception) {
            throw new OptionException(command, exception.getMessage());
        }
    }
}
